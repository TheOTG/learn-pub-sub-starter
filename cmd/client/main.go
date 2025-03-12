package main

import (
	"fmt"
	"log"
	"strconv"
	"time"

	"github.com/bootdotdev/learn-pub-sub-starter/internal/gamelogic"
	"github.com/bootdotdev/learn-pub-sub-starter/internal/pubsub"
	"github.com/bootdotdev/learn-pub-sub-starter/internal/routing"
	amqp "github.com/rabbitmq/amqp091-go"
)

func main() {
	fmt.Println("Starting Peril client...")

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

	username, err := gamelogic.ClientWelcome()
	if err != nil {
		fmt.Printf("error when getting username: %v", err)
		return
	}

	gs := gamelogic.NewGameState(username)

	err = pubsub.SubscribeJSON(
		conn,
		routing.ExchangePerilDirect,
		routing.PauseKey+"."+gs.GetUsername(),
		routing.PauseKey,
		pubsub.TRANSIENT,
		handlerPause(gs),
	)
	if err != nil {
		log.Fatalf("could not subscribe to pause: %v", err)
	}
	err = pubsub.SubscribeJSON(
		conn,
		routing.ExchangePerilTopic,
		routing.ArmyMovesPrefix+"."+gs.GetUsername(),
		routing.ArmyMovesPrefix+".*",
		pubsub.TRANSIENT,
		handlerMove(gs, amqpChan),
	)
	if err != nil {
		log.Fatalf("could not subscribe to army moves: %v", err)
	}
	err = pubsub.SubscribeJSON(
		conn,
		routing.ExchangePerilTopic,
		routing.WarRecognitionsPrefix,
		routing.WarRecognitionsPrefix+".*",
		pubsub.DURABLE,
		handlerWar(gs, amqpChan),
	)
	if err != nil {
		log.Fatalf("could not subscribe to war: %v", err)
	}

	for {
		input := gamelogic.GetInput()
		if len(input) == 0 {
			continue
		}

		switch input[0] {
		case "spawn":
			gs.CommandSpawn(input)
		case "move":
			armyMove, err := gs.CommandMove(input)
			if err != nil {
				fmt.Println(err)
				continue
			}

			pubsub.PublishJSON(
				amqpChan,
				routing.ExchangePerilTopic,
				routing.ArmyMovesPrefix+"."+gs.GetUsername(),
				armyMove,
			)
			fmt.Println("Move published successfully!")
		case "status":
			gs.CommandStatus()
		case "help":
			gamelogic.PrintClientHelp()
		case "spam":
			if len(input) < 2 {
				continue
			}

			num, err := strconv.Atoi(input[1])
			if err != nil {
				fmt.Println(err)
				continue
			}

			for range num {
				maliciousLog := gamelogic.GetMaliciousLog()
				err := pubsub.PublishGob(
					amqpChan,
					routing.ExchangePerilTopic,
					routing.GameLogSlug+"."+gs.GetUsername(),
					routing.GameLog{
						CurrentTime: time.Now(),
						Message:     maliciousLog,
						Username:    gs.GetUsername(),
					},
				)
				if err != nil {
					fmt.Printf("cannot publish malicious log: %s", err)
					continue
				}
			}
		case "quit":
			gamelogic.PrintQuit()
			return
		default:
			fmt.Println("Invalid command")
		}
	}
}
