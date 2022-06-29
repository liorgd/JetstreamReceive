package main

import (
	"context"
	"encoding/json"
	"log"
	"time"

	"github.com/nats-io/nats.go"
)

const (
	subSubjectName = "ORDERS.created"
	pubSubjectName = "ORDERS.approved"
)

type Order struct {
	OrderID    int
	CustomerID string
	Status     string
}

func main() {
	log.Println("Connect to NATS")
	nc, _ := nats.Connect("demo.nats.io")
	log.Println("Creates JetStreamContext")
	js, err := nc.JetStream()
	checkErr(err)
	log.Println("Create Pull based consumer with maximum 128 inflight.")
	sub, _ := js.PullSubscribe(subSubjectName, "orderReviewSubscriber", nats.PullMaxWaiting(128))
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	for {
		select {
		case <-ctx.Done():
			checkErr(err)
		default:
		}
		msgs, _ := sub.Fetch(10, nats.Context(ctx))
		for _, msg := range msgs {
			msg.Ack()
			var order Order
			err := json.Unmarshal(msg.Data, &order)
			checkErr(err)
			log.Printf("Subscriber fetched msg.Data:%s from subSubjectName:%q", string(msg.Data), msg.Subject)
			reviewOrder(js, order)
		}
	}
}

func reviewOrder(js nats.JetStreamContext, order Order) {
	// Changing the Order status
	order.Status = "approved"
	orderJSON, _ := json.Marshal(order)
	_, err := js.Publish(pubSubjectName, orderJSON)
	checkErr(err)
	log.Printf("Published orderJSON:%s to subjectName:%q", string(orderJSON), pubSubjectName)
}

func checkErr(err error) {
	if err != nil {
		log.Panic(err)
	}
}
