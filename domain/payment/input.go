package payment

type PostInput struct {
	CorrelationId string  `json:"correlationId"`
	Amount        float64 `json:"amount"`
}
