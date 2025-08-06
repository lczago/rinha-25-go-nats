package service

import (
	"fmt"
	"net/http"
	"os"
	"time"

	"github.com/goccy/go-json"
	"github.com/gofiber/fiber/v2/log"
)

type IPaymentProcessor interface {
	Process(input PostPaymentProcessor) (ProcessorType, error)
}

type service struct {
	Processors []*processor
}

func NewPaymentProcessorService() IPaymentProcessor {
	defaultUrl := os.Getenv("PROCESSOR_DEFAULT_URL")
	fallBackUrl := os.Getenv("PROCESSOR_FALLBACK_URL")

	processors := []*processor{
		{
			url:       defaultUrl,
			orderType: ProcessorTypeDefault,
		},
		{
			url:       fallBackUrl,
			orderType: ProcessorTypeFallback,
		},
	}

	s := &service{processors}
	go func() {
		ticker := time.NewTicker(time.Second * 5)
		for range ticker.C {
			if err := s.CheckHealth(); err != nil {
				log.Error(err)
			}
		}
	}()

	return s
}

func (s *service) CheckHealth() error {
	var healthStatus Health
	for _, p := range s.Processors {
		req, err := http.NewRequest(http.MethodGet, fmt.Sprintf("%s/payments/service-health", p.url), nil)
		if err != nil {
			return err
		}

		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			return err
		}
		defer resp.Body.Close()

		err = json.NewDecoder(resp.Body).Decode(&healthStatus)
		if err != nil {
			return err
		}

		p.health = !healthStatus.Failling
		p.minRespTime = healthStatus.MinResponseTime

		if resp.StatusCode == http.StatusInternalServerError {
			return ErrInternalServerError
		}
	}
	return nil
}

func (s *service) Process(input PostPaymentProcessor) (ProcessorType, error) {
	var lastRespTime = s.Processors[1].minRespTime
	for _, p := range s.Processors {
		if !p.health {
			continue
		}

		if p.minRespTime < 100 || (lastRespTime*2) < p.minRespTime {
			if err := p.processWithClient(input); err != nil {
				return p.orderType, err
			}
			return p.orderType, nil
		}
		lastRespTime = p.minRespTime
	}
	return ProcessorTypeNone, ErrInternalServerError
}
