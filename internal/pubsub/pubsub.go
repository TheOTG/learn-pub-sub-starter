package pubsub

import (
	"bytes"
	"context"
	"encoding/gob"
	"encoding/json"
	"log"

	amqp "github.com/rabbitmq/amqp091-go"
)

const (
	TRANSIENT = 0
	DURABLE   = 1
)

type AckType int

const (
	Ack AckType = iota
	NackRequeue
	NackDiscard
)

func PublishJSON[T any](ch *amqp.Channel, exchange, key string, val T) error {
	b, err := json.Marshal(val)
	if err != nil {
		return err
	}

	return ch.PublishWithContext(context.TODO(), exchange, key, false, false, amqp.Publishing{ContentType: "application/json", Body: b})
}

func PublishGob[T any](ch *amqp.Channel, exchange, key string, val T) error {
	var buffer bytes.Buffer
	enc := gob.NewEncoder(&buffer)
	err := enc.Encode(val)
	if err != nil {
		return err
	}

	return ch.PublishWithContext(context.TODO(), exchange, key, false, false, amqp.Publishing{ContentType: "application/gob", Body: buffer.Bytes()})
}

func DeclareAndBind(
	conn *amqp.Connection,
	exchange,
	queueName,
	key string,
	simpleQueueType int, // an enum to represent "durable" or "transient"
) (*amqp.Channel, amqp.Queue, error) {
	amqpChan, err := conn.Channel()
	if err != nil {
		return nil, amqp.Queue{}, err
	}

	durable := simpleQueueType == DURABLE
	autoDelete := simpleQueueType == TRANSIENT
	exclusive := simpleQueueType == TRANSIENT

	queue, err := amqpChan.QueueDeclare(
		queueName,
		durable,
		autoDelete,
		exclusive,
		false,
		amqp.Table{"x-dead-letter-exchange": "peril_dlx"},
	)
	if err != nil {
		return nil, amqp.Queue{}, err
	}

	err = amqpChan.QueueBind(queueName, key, exchange, false, nil)
	if err != nil {
		return nil, amqp.Queue{}, err
	}

	return amqpChan, queue, nil
}

func SubscribeJSON[T any](
	conn *amqp.Connection,
	exchange,
	queueName,
	key string,
	simpleQueueType int,
	handler func(T) AckType,
) error {
	amqpChan, queue, err := DeclareAndBind(conn, exchange, queueName, key, simpleQueueType)
	if err != nil {
		return err
	}

	deliveryChan, err := amqpChan.Consume(queue.Name, "", false, false, false, false, amqp.Table{})
	if err != nil {
		return err
	}

	go func() {
		for delivery := range deliveryChan {
			var data T
			err = json.Unmarshal(delivery.Body, &data)
			if err != nil {
				log.Printf("Error unmarshaling message: %v\n", err)
				delivery.Nack(false, false)
				continue
			}

			acktype := handler(data)

			switch acktype {
			case Ack:
				delivery.Ack(false)
			case NackRequeue:
				delivery.Nack(false, true)
			case NackDiscard:
				delivery.Nack(false, false)
			default:
				delivery.Nack(false, false)
			}
		}
	}()

	return nil
}

func SubscribeGob[T any](
	conn *amqp.Connection,
	exchange,
	queueName,
	key string,
	simpleQueueType int,
	handler func(T) AckType,
) error {
	amqpChan, queue, err := DeclareAndBind(conn, exchange, queueName, key, simpleQueueType)
	if err != nil {
		return err
	}

	err = amqpChan.Qos(10, 0, false)
	if err != nil {
		return err
	}

	deliveryChan, err := amqpChan.Consume(queue.Name, "", false, false, false, false, amqp.Table{})
	if err != nil {
		return err
	}

	go func() {
		for delivery := range deliveryChan {
			var data T
			buffer := bytes.NewBuffer(delivery.Body)
			enc := gob.NewDecoder(buffer)
			err = enc.Decode(&data)
			if err != nil {
				log.Printf("Error decoding message: %v\n", err)
				delivery.Nack(false, false)
				continue
			}

			acktype := handler(data)

			switch acktype {
			case Ack:
				delivery.Ack(false)
			case NackRequeue:
				delivery.Nack(false, true)
			case NackDiscard:
				delivery.Nack(false, false)
			default:
				delivery.Nack(false, false)
			}
		}
	}()

	return nil
}
