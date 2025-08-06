package payment

import (
	"context"
	"errors"
	"os"
	"rinha-25-go-nats/infrastructure/queue"
	"rinha-25-go-nats/infrastructure/service"
	"strconv"
	"time"

	"github.com/goccy/go-json"
	"github.com/nats-io/nats.go"
)

const (
	consumerQueue = "payment-processor"

	defaultMaxAckPending = 40
	defaultAckWait       = 30 * time.Second
	defaultMaxDeliver    = 3
)

type batchItem struct {
	msg       *nats.Msg
	entity    Entity
	processed bool
}

type IConsumer interface {
	StartProcess() error
	Close()
}

type natsConsumer struct {
	paymentQueue            *queue.PaymentQueue
	repository              IRepository
	paymentProcessorService service.IPaymentProcessor
	ctx                     context.Context
	cancelCtx               context.CancelFunc
	maxAckPending           int
	maxDeliver              int
}

func NewNatsConsumer(
	paymentQueue *queue.PaymentQueue, repository IRepository, paymentProcessorService service.IPaymentProcessor,
) IConsumer {
	maxAckPending := defaultMaxAckPending
	maxAckPendingStr := os.Getenv("NATS_MAX_ACK_PENDING")
	if maxAckPendingStr != "" {
		if val, err := strconv.Atoi(maxAckPendingStr); err == nil && val > 0 {
			maxAckPending = val
		}
	}

	maxDeliver := defaultMaxDeliver
	maxDeliverStr := os.Getenv("NATS_MAX_DELIVER")
	if maxDeliverStr != "" {
		if val, err := strconv.Atoi(maxDeliverStr); err == nil && val > 0 {
			maxDeliver = val
		}
	}

	ctx, cancelCtx := context.WithCancel(context.Background())

	consumer := &natsConsumer{
		paymentQueue:            paymentQueue,
		repository:              repository,
		paymentProcessorService: paymentProcessorService,
		ctx:                     ctx,
		cancelCtx:               cancelCtx,
		maxAckPending:           maxAckPending,
		maxDeliver:              maxDeliver,
	}

	return consumer
}

func (c *natsConsumer) StartProcess() error {
	sub, err := c.paymentQueue.JetStream.QueueSubscribeSync(
		c.paymentQueue.Subject,
		consumerQueue,
		nats.AckWait(defaultAckWait),
		nats.ManualAck(),
		nats.DeliverAll(),
		nats.ReplayInstant(),
		nats.MaxDeliver(c.maxDeliver),
		nats.MaxAckPending(c.maxAckPending),
	)
	if err != nil {
		return err
	}
	defer sub.Unsubscribe()

	for {
		select {
		case <-c.ctx.Done():
			return nil
		default:
			msg, err := sub.NextMsgWithContext(c.ctx)
			if err != nil {
				continue
			}
			go c.processMessage(msg)
		}
	}
}

func (c *natsConsumer) processMessage(msg *nats.Msg) {
	var payment PostInput
	if err := json.Unmarshal(msg.Data, &payment); err != nil {
		msg.Term()
		return
	}

	paymentProcessorModel := service.PostPaymentProcessor{
		CorrelationId: payment.CorrelationId,
		Amount:        payment.Amount,
		RequestedAt:   time.Now().UTC(),
	}

	processorType, err := c.paymentProcessorService.Process(paymentProcessorModel)
	if err != nil {
		if errors.Is(err, service.ErrUnprocessableEntity) {
			msg.Term()
			return
		}
		msg.NakWithDelay(time.Second * 5)
		return
	}

	entity := Entity{
		CorrelationId: payment.CorrelationId,
		ProcessedBy:   string(processorType),
		Amount:        payment.Amount,
		ProcessedAt:   paymentProcessorModel.RequestedAt,
	}

	if err = c.repository.Insert(c.ctx, entity); err != nil {
		msg.Term()
		return
	}

	msg.Ack()
	return
}

func (c *natsConsumer) Close() {
	c.cancelCtx()
}
