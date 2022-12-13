package main

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/url"
	"os"
	"runtime"
	"strings"
	"time"

	"github.com/PuerkitoBio/goquery"
	amqp "github.com/rabbitmq/amqp091-go"
	"gopkg.in/yaml.v3"
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
}

func main() {
	time.Sleep(30 * time.Second)
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

	q, err := ch.QueueDeclare(
		"url-frontier", // name
		false,          // durable
		false,          // delete when unused
		false,          // exclusive
		false,          // no-wait
		nil,            // arguments
	)

	if err != nil {
		log.Fatal(err)
	}
	log.Println("rabbitmq queue url-frontier successful")

	qBack, err := ch.QueueDeclare(
		"back", // name
		false,  // durable
		false,  // delete when unused
		false,  // exclusive
		false,  // no-wait
		nil,    // arguments
	)

	if err != nil {
		log.Fatal(err)
	}
	log.Println("rabbitmq queue url-back successful")

	qPer, err := ch.QueueDeclare(
		"persist", // name
		false,     // durable
		false,     // delete when unused
		false,     // exclusive
		false,     // no-wait
		nil,       // arguments
	)
	if err != nil {
		log.Fatal(err)
	}

	log.Println("rabbitmq queue persist successful")

	msgs, err := ch.Consume(
		q.Name, // queue
		"",     // consumer
		true,   // auto-ack
		false,  // exclusive
		false,  // no-local
		false,  // no-wait
		nil,    // args
	)

	if err != nil {
		log.Fatal(err)
	}

	var forever chan struct{}
	client := &http.Client{}
	go func() {
		for d := range msgs {
			u := config.Protocol + "://" + string(d.Body)
			log.Printf("Received: %s\n", d.Body)
			req, _ := http.NewRequest("GET", u, nil)
			req.Header.Set("User-Agent", config.UserAgent)
			resp, _ := client.Do(req)
			defer resp.Body.Close()
			if resp.StatusCode != 200 {
				log.Printf("[ERROR] failed to fetch data: %d %s\n", resp.StatusCode, resp.Status)
			}
			buf, _ := io.ReadAll(resp.Body)
			rdr1 := io.NopCloser(bytes.NewBuffer(buf))
			rdr2 := io.NopCloser(bytes.NewBuffer(buf))

			resp.Body = rdr1
			doc, err := goquery.NewDocumentFromReader(resp.Body)

			if err != nil {
				log.Println("[ERROR] ", err)
			}
			var links []string
			doc.Find("body a").Each(func(_ int, s *goquery.Selection) {
				link, ok := s.Attr("href")
				if !ok {
					return
				}
				if strings.Contains(link, "http") {
					ur, _ := url.Parse(link)
					link = ur.Hostname()
					links = append(links, link)
				}
			})
			fmt.Println(">>>", links)
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
