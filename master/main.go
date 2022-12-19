package main

import (
	"context"
	amqp "github.com/rabbitmq/amqp091-go"
	"log"
	"strings"
	"time"

	redis "github.com/go-redis/redis/v8"
)

const InitialDomain = "wikipedia.com"

func main() {
	time.Sleep(30 * time.Second)
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

	err = ch.PublishWithContext(context.TODO(),
		"",     // exchange
		q.Name, // routing key
		false,  // mandatory
		false,  // immediate
		amqp.Publishing{
			ContentType: "text/plain",
			Body:        []byte(InitialDomain),
		},
	)

	if err != nil {
		log.Fatal(err, "82")
	}

	rdb.Set(context.TODO(), InitialDomain, true, 0)
	log.Println("Initial message sent")

	msgs, err := ch.Consume(
		qBack.Name, // queue
		"",         // consumer
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
		}
	}()

	log.Printf(" [*] Waiting for messages. To exit press CTRL+C\n")
	<-forever
}
