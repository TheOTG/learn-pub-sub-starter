package main

import (
	"fmt"

	"github.com/bootdotdev/learn-pub-sub-starter/internal/gamelogic"
	"github.com/bootdotdev/learn-pub-sub-starter/internal/pubsub"
	"github.com/bootdotdev/learn-pub-sub-starter/internal/routing"
)

func handlerLog() func(routing.GameLog) pubsub.AckType {
	return func(gl routing.GameLog) pubsub.AckType {
		defer fmt.Print("> ")
		err := gamelogic.WriteLog(gl)
		if err != nil {
			return pubsub.NackRequeue
		}
		return pubsub.Ack
	}
}
