package payment

import (
	"github.com/gofiber/fiber/v2"
	"rinha-25-go-nats/infrastructure/queue"
)

type Controller struct {
	paymentQueue *queue.PaymentQueue
	repository   IRepository
}

func NewController(paymentQueue *queue.PaymentQueue, repository IRepository) *Controller {
	return &Controller{paymentQueue, repository}
}

func (c *Controller) InitRoutes(app *fiber.App) {
	app.Post("/payments", c.postPayment)
	app.Get("/payments-summary", c.getSummary)
	app.Post("/purge-payments", c.purge)
}

func (c *Controller) postPayment(ctx *fiber.Ctx) error {
	if _, err := c.paymentQueue.JetStream.PublishAsync(c.paymentQueue.Subject, ctx.BodyRaw()); err != nil {
		return ctx.SendStatus(fiber.StatusInternalServerError)
	}
	return ctx.SendStatus(fiber.StatusOK)
}

func (c *Controller) getSummary(ctx *fiber.Ctx) error {
	var summaryDate SummaryDate
	if err := ctx.QueryParser(&summaryDate); err != nil {
		return ctx.SendStatus(fiber.StatusBadRequest)
	}

	summary, err := c.repository.AggregateSummary(ctx.Context(), summaryDate)
	if err != nil {
		return ctx.SendStatus(fiber.StatusInternalServerError)
	}

	return ctx.Status(fiber.StatusOK).JSON(summary)
}

func (c *Controller) purge(ctx *fiber.Ctx) error {
	if err := c.repository.DeleteAll(ctx.Context()); err != nil {
		return ctx.SendStatus(fiber.StatusInternalServerError)
	}

	if err := c.paymentQueue.JetStream.PurgeStream(c.paymentQueue.StreamName); err != nil {
		return ctx.SendStatus(fiber.StatusInternalServerError)
	}

	return ctx.SendStatus(fiber.StatusOK)
}
