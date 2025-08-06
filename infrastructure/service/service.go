package service

import (
	"fmt"
	"net/http"
	"os"
	"sync/atomic"

	"github.com/goccy/go-json"
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
			health:    atomic.Bool{},
			url:       defaultUrl,
			orderType: ProcessorTypeDefault,
		},
		{
			health:    atomic.Bool{},
			url:       fallBackUrl,
			orderType: ProcessorTypeFallback,
		},
	}
	for _, p := range processors {
		go p.resetHealth()
	}
	return &service{processors}
}

func (s *service) CheckHealth() error {
	var healthStatus Health
	for _, p := range s.Processors {
		if p.health.Load() {
			continue
		}
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

		p.health.Store(!healthStatus.Failling)
		p.minRespTime = healthStatus.MinResponseTime

		if resp.StatusCode == http.StatusInternalServerError {
			return ErrInternalServerError
		}
	}
	return nil
}

func (s *service) Process(input PostPaymentProcessor) (ProcessorType, error) {
	if err := s.CheckHealth(); err != nil {
		return ProcessorTypeNone, err
	}
	var lastRespTime = s.Processors[1].minRespTime
	for _, p := range s.Processors {
		if !p.health.Load() {
			continue
		}

		if p.minRespTime < 100 || lastRespTime < p.minRespTime {
			if err := p.processWithClient(input); err != nil {
				return p.orderType, err
			}
			return p.orderType, nil
		}
		lastRespTime = p.minRespTime
	}
	return ProcessorTypeNone, ErrInternalServerError
}
