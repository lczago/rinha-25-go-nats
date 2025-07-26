package payment

import (
	"context"
	"errors"
	"github.com/goccy/go-json"
	"github.com/gofiber/fiber/v2/log"
	"github.com/lib/pq"
	"github.com/nats-io/nats.go"
	"os"
	"rinha-25-go-nats/infrastructure/queue"
	"rinha-25-go-nats/infrastructure/service"
	"strconv"
	"time"
)

const (
	consumerQueue = "payment-processor"
)

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
	limit                   int
}

func NewNatsConsumer(
	paymentQueue *queue.PaymentQueue, repository IRepository, paymentProcessorService service.IPaymentProcessor,
) IConsumer {
	var limit = 30
	limitQuantity := os.Getenv("CONSUMER_LIMIT")
	if limitQuantity != "" {
		limit, _ = strconv.Atoi(limitQuantity)
	}
	ctx, cancelCtx := context.WithCancel(context.Background())
	return &natsConsumer{
		paymentQueue,
		repository,
		paymentProcessorService,
		ctx,
		cancelCtx,
		limit,
	}
}

func (c *natsConsumer) StartProcess() error {
	sub, err := c.paymentQueue.JetStream.QueueSubscribeSync(
		c.paymentQueue.Subject,
		consumerQueue,
		nats.AckWait(time.Second*30),
		nats.ManualAck(),
		nats.DeliverAll(),
		nats.ReplayInstant(),
		nats.MaxDeliver(3),
		nats.MaxAckPending(c.limit),
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
				log.Error(err)
				continue
			}
			go func() {
				if err := c.processMessage(msg); err != nil {
					log.Error(err)
				}
			}()
		}
	}

}

func (c *natsConsumer) processMessage(msg *nats.Msg) error {
	var payment PostInput
	if err := json.UnmarshalNoEscape(msg.Data, &payment); err != nil {
		return err
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
			return nil
		}
		msg.NakWithDelay(time.Second * 5)
		return nil
	}

	entity := Entity{
		CorrelationId: payment.CorrelationId,
		ProcessedBy:   string(processorType),
		Amount:        payment.Amount,
		ProcessedAt:   paymentProcessorModel.RequestedAt,
	}
	if err = c.repository.Insert(c.ctx, entity); err != nil {
		var pgErr *pq.Error
		if errors.As(err, &pgErr) && pgErr.Code == "23505" {
			msg.Term()
			return nil
		}
		msg.NakWithDelay(time.Second * 5)
		return nil
	}

	msg.Ack()
	return nil
}

func (c *natsConsumer) Close() {
	c.cancelCtx()
}
