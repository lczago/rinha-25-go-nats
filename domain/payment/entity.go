package payment

import (
	"time"
)

type Entity struct {
	CorrelationId string    `db:"correlation_id"`
	ProcessedBy   string    `db:"processed_by"`
	Amount        float64   `db:"amount"`
	ProcessedAt   time.Time `db:"processed_at"`
}
