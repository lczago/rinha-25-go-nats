package main

import (
	"os"
	"rinha-25-go-nats/domain/payment"
	"rinha-25-go-nats/infrastructure/database"
	"rinha-25-go-nats/infrastructure/queue"
	"rinha-25-go-nats/infrastructure/service"
	"runtime"
	"runtime/debug"

	"github.com/goccy/go-json"
	"github.com/gofiber/fiber/v2"
	"github.com/gofiber/fiber/v2/log"
)

const defaultPort = "9999"

func main() {
	runtime.GOMAXPROCS(runtime.NumCPU())

	debug.SetGCPercent(500)

	log.SetLevel(log.LevelError)

	cfg := fiber.Config{
		JSONEncoder:           json.Marshal,
		JSONDecoder:           json.Unmarshal,
		Concurrency:           2048,
		DisableStartupMessage: true,
		EnablePrintRoutes:     false,
		ReduceMemoryUsage:     false,
		BodyLimit:             1 * 1024 * 1024,
		StreamRequestBody:     true,
		DisableKeepalive:      false,
	}
	api := fiber.New(cfg)

	db, err := database.NewRedis()
	if err != nil {
		log.Fatal(err)
	}

	paymentQueue, err := queue.NewPaymentQueue()
	if err != nil {
		log.Fatal(err)
	}

	paymentRepo := payment.NewRepository(db)
	payment.NewController(paymentQueue, paymentRepo).InitRoutes(api)

	paymentProcessorService := service.NewPaymentProcessorService()
	consumer := payment.NewNatsConsumer(paymentQueue, paymentRepo, paymentProcessorService)
	defer consumer.Close()

	go func() {
		if err := consumer.StartProcess(); err != nil {
			log.Fatal(err)
		}
	}()

	port := os.Getenv("SERVER_PORT")
	if port == "" {
		port = defaultPort
	}

	if err = api.Listen(":" + port); err != nil {
		log.Fatal(err)
	}
}
