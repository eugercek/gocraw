package main

import (
	"database/sql"
	"github.com/eugercek/gocraw/internal/rabmq"
	_ "github.com/lib/pq"
	amqp "github.com/rabbitmq/amqp091-go"
	"log"
	"strings"
	"time"
)

const connStr = "postgresql://postgres:pass@postgre/postgres?sslmode=disable"

func main() {
	time.Sleep(50 * time.Second)
	db, err := sql.Open("postgres", connStr)

	if err != nil {
		log.Fatal(err)
	}
	_, err = db.Exec(`
DROP table IF EXISTS links;
CREATE TABLE IF NOT EXISTS links (
  id SERIAL PRIMARY KEY,
  dname varchar(255) NOT NULL,
  html text,
  time_stamp timestamptz
);
`) // TODO
	if err != nil {
		log.Fatal(err)
	}
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

	q, err := rabmq.QueueDeclare(ch, "persist")
	if err != nil {
		log.Fatal(err)
	}
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

	stmnt, err := db.Prepare(`
INSERT into links(dname, html, time_stamp)
VALUES ($1, $2, $3)
`)
	if err != nil {
		log.Fatal(err)
	}
	defer stmnt.Close()

	go func() {
		for msg := range msgs {
			domain, html := splitDomainHtml(string(msg.Body))
			log.Println("Persisting new record ", domain)

			_, err := stmnt.Exec(domain, html, time.Now())
			if err != nil {
				log.Fatal(err)
			}

		}
	}()

	log.Printf(" [*] Waiting for messages. To exit press CTRL+C\n")
	<-forever
}

func splitDomainHtml(str string) (string, string) {
	i := strings.Index(str, ",")
	return str[0:i], str[i+1:]
}
