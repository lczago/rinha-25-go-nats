package payment

import (
	"context"
	"errors"
	"github.com/goccy/go-json"
	"github.com/gofiber/fiber/v2/log"
	"github.com/nats-io/nats.go"
	"rinha-25-go-nats/infrastructure/queue"
	"rinha-25-go-nats/infrastructure/service"
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
}

func NewNatsConsumer(
	paymentQueue *queue.PaymentQueue, repository IRepository, paymentProcessorService service.IPaymentProcessor,
) IConsumer {
	ctx, cancelCtx := context.WithCancel(context.Background())
	return &natsConsumer{
		paymentQueue,
		repository,
		paymentProcessorService,
		ctx,
		cancelCtx,
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
		nats.MaxDeliver(2),
		nats.MaxAckPending(40),
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
			go c.processMessage(msg)
		}
	}

}

func (c *natsConsumer) processMessage(msg *nats.Msg) {
	var payment PostInput
	if err := json.Unmarshal(msg.Data, &payment); err != nil {
		log.Error(err)
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
		msg.NakWithDelay(time.Second * 3)
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
