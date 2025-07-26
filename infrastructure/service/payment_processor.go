package service

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"github.com/goccy/go-json"
	"net/http"
	"os"
	"time"
)

type ProcessorType string

const (
	ProcessorTypeDefault  ProcessorType = "DEFAULT"
	ProcessorTypeFallback ProcessorType = "FALLBACK"
)

type IPaymentProcessor interface {
	Process(input PostPaymentProcessor) (ProcessorType, error)
}

type paymentProcessor struct {
	defaultUrl  string
	fallBackUrl string
}

func NewPaymentProcessorService() IPaymentProcessor {
	defaultUrl := os.Getenv("PROCESSOR_DEFAULT_URL")
	fallBackUrl := os.Getenv("PROCESSOR_FALLBACK_URL")

	return &paymentProcessor{defaultUrl, fallBackUrl}
}

func (s *paymentProcessor) Process(input PostPaymentProcessor) (ProcessorType, error) {
	pt, err := s.processWithClient(s.defaultUrl, input, ProcessorTypeDefault)
	if err != nil {
		return s.processWithClient(s.fallBackUrl, input, ProcessorTypeFallback)
	}
	return pt, nil
}

func (s *paymentProcessor) processWithClient(
	baseURL string,
	input PostPaymentProcessor,
	processorType ProcessorType,
) (ProcessorType, error) {
	ctx, cancelCtx := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancelCtx()

	body, _ := json.Marshal(input)
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, fmt.Sprintf("%s/payments", baseURL), bytes.NewBuffer(body))
	if err != nil {
		return "", err
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusUnprocessableEntity {
		return "", ErrUnprocessableEntity
	}

	if resp.StatusCode >= 400 {
		return "", errors.New("")
	}

	return processorType, nil
}
