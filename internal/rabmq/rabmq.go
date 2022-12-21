package rabmq

import (
	"github.com/rabbitmq/amqp091-go"
	"log"
)

func QueueDeclare(ch *amqp091.Channel, name string) (*amqp091.Queue, error) {
	q, err := ch.QueueDeclare(
		name,  // name
		false, // durable
		false, // delete when unused
		false, // exclusive
		false, // no-wait
		nil,   // arguments
	)

	if err != nil {
		return nil, err
	}

	log.Printf("rabbitmq queue %s successful\n", name)
	return &q, nil
}
