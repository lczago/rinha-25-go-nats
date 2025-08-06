package service

import "time"

type ProcessorType string

const (
	ProcessorTypeNone     ProcessorType = ""
	ProcessorTypeDefault  ProcessorType = "DEFAULT"
	ProcessorTypeFallback ProcessorType = "FALLBACK"
)

type PostPaymentProcessor struct {
	CorrelationId string    `json:"correlationId"`
	Amount        float64   `json:"amount"`
	RequestedAt   time.Time `json:"requestedAt"`
}

type Health struct {
	Failling        bool `json:"failling"`
	MinResponseTime int  `json:"minResponseTime"`
}
