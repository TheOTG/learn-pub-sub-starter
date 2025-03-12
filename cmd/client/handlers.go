package main

import (
	"fmt"
	"time"

	"github.com/bootdotdev/learn-pub-sub-starter/internal/gamelogic"
	"github.com/bootdotdev/learn-pub-sub-starter/internal/pubsub"
	"github.com/bootdotdev/learn-pub-sub-starter/internal/routing"

	amqp "github.com/rabbitmq/amqp091-go"
)

func handlerPause(gs *gamelogic.GameState) func(routing.PlayingState) pubsub.AckType {
	return func(ps routing.PlayingState) pubsub.AckType {
		defer fmt.Print("> ")
		gs.HandlePause(ps)
		return pubsub.Ack
	}
}

func handlerMove(gs *gamelogic.GameState, ch *amqp.Channel) func(gamelogic.ArmyMove) pubsub.AckType {
	return func(move gamelogic.ArmyMove) pubsub.AckType {
		defer fmt.Print("> ")
		outcome := gs.HandleMove(move)
		switch outcome {
		case gamelogic.MoveOutComeSafe:
			return pubsub.Ack
		case gamelogic.MoveOutcomeMakeWar:
			err := pubsub.PublishJSON(
				ch,
				routing.ExchangePerilTopic,
				routing.WarRecognitionsPrefix+"."+gs.GetUsername(),
				gamelogic.RecognitionOfWar{
					Attacker: move.Player,
					Defender: gs.GetPlayerSnap(),
				},
			)
			if err != nil {
				fmt.Printf("error when publishing war message: %s\n", err)
				return pubsub.NackRequeue
			}
			return pubsub.Ack
		case gamelogic.MoveOutcomeSamePlayer:
			return pubsub.NackDiscard
		default:
			return pubsub.NackDiscard
		}
	}
}

func handlerWar(gs *gamelogic.GameState, ch *amqp.Channel) func(gamelogic.RecognitionOfWar) pubsub.AckType {
	return func(war gamelogic.RecognitionOfWar) pubsub.AckType {
		defer fmt.Print("> ")
		outcome, winner, loser := gs.HandleWar(war)

		gamelog := routing.GameLog{
			CurrentTime: time.Now(),
			Message:     "",
			Username:    gs.GetUsername(),
		}

		switch outcome {
		case gamelogic.WarOutcomeNotInvolved:
			return pubsub.NackRequeue
		case gamelogic.WarOutcomeNoUnits:
			fmt.Println("No unit outcome")
			return pubsub.NackDiscard
		case gamelogic.WarOutcomeYouWon:
			gamelog.Message = fmt.Sprintf("%s won a war against %s", winner, loser)
			err := pubsub.PublishGob(
				ch,
				routing.ExchangePerilTopic,
				routing.GameLogSlug+"."+gs.GetUsername(),
				gamelog,
			)
			if err != nil {
				fmt.Printf("error when logging war message: %s\n", err)
				return pubsub.NackRequeue
			}
			return pubsub.Ack
		case gamelogic.WarOutcomeOpponentWon:
			gamelog.Message = fmt.Sprintf("%s won a war against %s", winner, loser)
			err := pubsub.PublishGob(
				ch,
				routing.ExchangePerilTopic,
				routing.GameLogSlug+"."+gs.GetUsername(),
				gamelog,
			)
			if err != nil {
				fmt.Printf("error when logging war message: %s\n", err)
				return pubsub.NackRequeue
			}
			return pubsub.Ack
		case gamelogic.WarOutcomeDraw:
			gamelog.Message = fmt.Sprintf("A war between %s and %s resulted in a draw", winner, loser)
			err := pubsub.PublishGob(
				ch,
				routing.ExchangePerilTopic,
				routing.GameLogSlug+"."+gs.GetUsername(),
				gamelog,
			)
			if err != nil {
				fmt.Printf("error when logging war message: %s\n", err)
				return pubsub.NackRequeue
			}
			return pubsub.Ack
		default:
			return pubsub.NackDiscard
		}
	}
}
