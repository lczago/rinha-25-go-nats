package queue

import (
	"github.com/gofiber/fiber/v2/log"
	"github.com/nats-io/nats.go"
	"os"
	"time"
)

const (
	subject    = "payments"
	streamName = "Payments-Processor"
)

type PaymentQueue struct {
	JetStream  nats.JetStreamContext
	NatsConn   *nats.Conn
	Subject    string
	StreamName string
}

func NewPaymentQueue() (*PaymentQueue, error) {
	natsURL := os.Getenv("NATS_URL")
	if natsURL == "" {
		natsURL = nats.DefaultURL
	}

	natsConn, err := nats.Connect(natsURL)
	if err != nil {
		return nil, err
	}

	js, err := natsConn.JetStream()
	if err != nil {
		return nil, err
	}

	queue := &PaymentQueue{
		NatsConn:   natsConn,
		JetStream:  js,
		Subject:    subject,
		StreamName: streamName,
	}

	if err = queue.createStream(); err != nil {
		return nil, err
	}
	return queue, nil

}

func (q *PaymentQueue) createStream() error {
	now := time.Now().UTC()
	streamCfg := nats.StreamConfig{
		Name:      streamName,
		Subjects:  []string{subject},
		Retention: nats.WorkQueuePolicy,
		Storage:   nats.MemoryStorage,
		Replicas:  0,
	}
	stream, err := q.JetStream.AddStream(&streamCfg)
	if err != nil {
		return err
	}

	if stream.Created.After(now) {
		log.Info("Stream created")
	}
	return nil
}
