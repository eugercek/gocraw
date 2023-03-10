package main

import (
	"bytes"
	"context"
	"crypto/rand"
	"github.com/PuerkitoBio/goquery"
	"github.com/eugercek/gocraw/internal/rabmq"
	amqp "github.com/rabbitmq/amqp091-go"
	"gopkg.in/yaml.v3"
	"io"
	"log"
	"net/http"
	"net/url"
	"os"
	"runtime"
	"strings"
	"time"
)

type Config struct {
	// How many vCPU should be used, default is all
	CpuCount int `yaml:"cpu,omitempty"`

	// Maximum recursion depth of the crawling
	MaxDepth int

	// Default user agent
	UserAgent string

	// Resolving dns can bottleneck, a local dns can fasten the crawling
	DnsAddress string

	// Can be http or https
	Protocol string

	//
	TimeOut time.Duration
}

func main() {
	time.Sleep(50 * time.Second)
	name := generate(5)
	// confFile := flag.String("conf", "conf.yaml", "config file to use")
	// isConf := flag.Bool("-noconf", true, "Use default")

	// flag.Parse()

	config, err := LoadConfig("TODO", false)

	conn, err := amqp.Dial("amqp://guest:guest@rabbit:5672/")
	if err != nil {
		log.Fatal(err)
	}
	log.Println("rabbitmq connection successful")

	if err != nil {
		log.Fatal(err)
	}
	defer conn.Close()

	ch, err := conn.Channel()

	if err != nil {
		log.Fatal(err)
	}
	log.Println("rabbitmq channel successful")
	defer ch.Close()

	qUrl, err := rabmq.QueueDeclare(ch, "url-frontier")
	if err != nil {
		log.Fatal(err)
	}

	qBack, err := rabmq.QueueDeclare(ch, "url-back")
	if err != nil {
		log.Fatal(err)
	}

	qPer, err := rabmq.QueueDeclare(ch, "persist")
	if err != nil {
		log.Fatal(err)
	}

	msgs, err := ch.Consume(
		qUrl.Name, // queue
		name,      // consumer
		true,      // auto-ack
		false,     // exclusive
		false,     // no-local
		false,     // no-wait
		nil,       // args
	)

	if err != nil {
		log.Fatal(err)
	}

	var forever chan struct{}
	client := &http.Client{}
	go func() {
		for d := range msgs {
			log.Printf("Upper received, %s", d.Body)
			go func(d amqp.Delivery) {
				u := config.Protocol + "://" + string(d.Body)
				log.Printf("Received: %s\n", d.Body)
				req, _ := http.NewRequest("GET", u, nil)
				req.Header.Set("User-Agent", config.UserAgent)
				resp, err := client.Do(req)
				if err != nil {
					log.Printf("[ERROR] http error: %s", err.Error())
					return
				}
				if resp.StatusCode != http.StatusOK {
					log.Printf("[ERROR] failed to fetch data: %d %s\n", resp.StatusCode, resp.Status)
					return
				}
				defer resp.Body.Close()
				buf, _ := io.ReadAll(resp.Body)
				rdr1 := io.NopCloser(bytes.NewBuffer(buf))
				rdr2 := io.NopCloser(bytes.NewBuffer(buf))

				resp.Body = rdr1
				doc, err := goquery.NewDocumentFromReader(resp.Body)

				if err != nil {
					log.Println("[ERROR] ", err)
					return
				}

				var links []string
				doc.Find("body a").Each(func(_ int, s *goquery.Selection) {
					link, ok := s.Attr("href")
					if !ok {
						return
					}
					if strings.Contains(link, config.Protocol) {
						ur, err := url.Parse(link)
						if err != nil {
							log.Printf("[ERROR] error in URL parse %s", err.Error())
							return
						}
						link = ur.Hostname()
						links = append(links, link)
					}
				})

				if len(links) > 0 {
					log.Printf("Found %d link from %s", len(links), u)
					err = ch.PublishWithContext(context.TODO(),
						"",         // exchange
						qBack.Name, // routing key
						false,      // mandatory
						false,      // immediate
						amqp.Publishing{
							ContentType: "text/plain",
							Body:        []byte(strings.Join(links, ",")),
						},
					)

					if err != nil {
						log.Println(err)
					}

				} else {
					log.Printf("Found no links from %s", links)
				}

				html, _ := io.ReadAll(rdr2)
				err = ch.PublishWithContext(context.TODO(),
					"",        // exchange
					qPer.Name, // routing key
					false,     // mandatory
					false,     // immediate
					amqp.Publishing{
						ContentType: "text/plain",
						Body:        []byte(string(d.Body) + "," + string(html)),
					},
				)

				if err != nil {
					log.Println(err)
				}
			}(d)
		}
	}()

	log.Printf(" [*] Waiting for messages. To exit press CTRL+C\n")
	<-forever
}

func LoadConfig(fname string, isConf bool) (*Config, error) {
	if !isConf {
		const GoogleBot = "Mozilla/5.0 (Linux; Android 6.0.1; Nexus 5X Build/MMB29P) AppleWebKit/537.36 (KHTML%2C like Gecko) Chrome/107.0.5304.115 Mobile Safari/537.36 (compatible; Googlebot/2.1; +http://www.google.com/bot.html)"
		return &Config{
			CpuCount:   runtime.NumCPU(),
			MaxDepth:   20,
			UserAgent:  GoogleBot,
			DnsAddress: "",
			Protocol:   "https",
		}, nil
	}

	var conf Config
	file, err := os.Open(fname)
	if err != nil {
		return nil, err
	}

	bs, err := io.ReadAll(file)

	if err != nil {
		return nil, err
	}

	err = yaml.Unmarshal(bs, &conf)

	if err != nil {
		return nil, err
	}

	return &conf, err
}

func generate(size int) string {
	alphabet := []byte("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ")
	b := make([]byte, size)
	rand.Read(b)
	for i := 0; i < size; i++ {
		b[i] = alphabet[b[i]%byte(len(alphabet))]
	}
	return string(b)
}
