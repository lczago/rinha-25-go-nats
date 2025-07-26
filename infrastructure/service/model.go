package service

import "time"

type PostPaymentProcessor struct {
	CorrelationId string    `json:"correlationId"`
	Amount        float64   `json:"amount"`
	RequestedAt   time.Time `json:"requestedAt"`
}
