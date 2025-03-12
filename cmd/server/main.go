package main

import (
	"fmt"
	"log"

	"github.com/bootdotdev/learn-pub-sub-starter/internal/gamelogic"
	"github.com/bootdotdev/learn-pub-sub-starter/internal/pubsub"
	"github.com/bootdotdev/learn-pub-sub-starter/internal/routing"
	amqp "github.com/rabbitmq/amqp091-go"
)

func main() {
	fmt.Println("Starting Peril server...")

	connString := "amqp://guest:guest@localhost:5672"
	conn, err := amqp.Dial(connString)
	if err != nil {
		fmt.Printf("error when connecting to RabbitMQ: %v", err)
		return
	}
	defer conn.Close()

	fmt.Println("Connection to RabbitMQ successful")

	amqpChan, err := conn.Channel()
	if err != nil {
		fmt.Printf("error when creating connection channel: %v", err)
		return
	}

	err = pubsub.SubscribeGob(
		conn,
		routing.ExchangePerilTopic,
		routing.GameLogSlug,
		routing.GameLogSlug+".*",
		pubsub.DURABLE,
		handlerLog(),
	)
	if err != nil {
		log.Fatalf("could not subscribe to game log: %v", err)
	}

	gamelogic.PrintServerHelp()

	for {
		input := gamelogic.GetInput()
		if len(input) == 0 {
			continue
		}

		switch input[0] {
		case "pause":
			fmt.Println("Pausing game..")
			pubsub.PublishJSON(amqpChan, routing.ExchangePerilDirect, routing.PauseKey, routing.PlayingState{IsPaused: true})
		case "resume":
			fmt.Println("Resuming game..")
			pubsub.PublishJSON(amqpChan, routing.ExchangePerilDirect, routing.PauseKey, routing.PlayingState{IsPaused: false})
		case "quit":
			fmt.Println("Exiting game..")
			return
		default:
			fmt.Println("Invalid command")
		}
	}
}
