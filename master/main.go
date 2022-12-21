package main

import (
	"context"
	"github.com/eugercek/gocraw/internal/rabmq"
	amqp "github.com/rabbitmq/amqp091-go"
	"log"
	"os"
	"strings"
	"time"

	redis "github.com/go-redis/redis/v8"
)

const SeedUrl = "www.wikipedia.org"

func main() {
	time.Sleep(50 * time.Second)
	seed := os.Getenv("SEED")
	if seed == "" {
		seed = SeedUrl
	}

	log.Printf("Starting with seed url %s\n", seed)
	rdb := redis.NewClient(&redis.Options{
		Addr:     "redis:6379",
		Password: "", // no password set
		DB:       0,  // use default DB
	})

	conn, err := amqp.Dial("amqp://guest:guest@rabbit:5672/")

	if err != nil {
		log.Fatal(err)
	}
	log.Println("rabbitmq connection successful")
	defer conn.Close()

	ch, err := conn.Channel()

	if err != nil {
		log.Fatal(err)
	}

	log.Println("rabbitmq channel successful")
	defer ch.Close()

	q, err := rabmq.QueueDeclare(ch, "url-frontier")
	if err != nil {
		log.Fatal(err)
	}

	qBack, err := rabmq.QueueDeclare(ch, "url-back")
	if err != nil {
		log.Fatal(err)
	}

	// Send seed url to worker nodes
	err = ch.PublishWithContext(context.TODO(),
		"",     // exchange
		q.Name, // routing key
		false,  // mandatory
		false,  // immediate
		amqp.Publishing{
			ContentType: "text/plain",
			Body:        []byte(SeedUrl),
		},
	)

	if err != nil {
		log.Fatal(err)
	}

	rdb.Set(context.TODO(), SeedUrl, true, 0)
	log.Println("Initial message sent")

	msgs, err := ch.Consume(
		qBack.Name, // queue
		"master",   // consumer
		true,       // auto-ack
		false,      // exclusive
		false,      // no-local
		false,      // no-wait
		nil,        // args
	)

	if err != nil {
		log.Fatal(err)
	}

	var forever chan struct{}

	go func() {
		for msg := range msgs {
			links := strings.Split(string(msg.Body), ",")
			log.Printf("Received %d message", len(links))
			var newLinks []string

			for _, link := range links {
				_, err := rdb.Get(context.TODO(), link).Result()

				if err == redis.Nil {
					rdb.Set(context.TODO(), link, true, 0).Err()
					newLinks = append(newLinks, link)
				} else if err != nil {
					log.Fatal(err)
				}
			}

			for _, link := range newLinks {
				err = ch.PublishWithContext(context.TODO(),
					"",     // exchange
					q.Name, // routing key
					false,  // mandatory
					false,  // immediate
					amqp.Publishing{
						ContentType: "text/plain",
						Body:        []byte(link),
					},
				)
				if err != nil {
					log.Print("[ERROR] ", err)
				}
			}
		}
	}()

	log.Printf(" [*] Waiting for messages. To exit press CTRL+C\n")
	<-forever
}
