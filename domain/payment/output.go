package payment

type ProcessorsSummary struct {
	Default  Summary `json:"default"`
	FallBack Summary `json:"fallback"`
}

type Summary struct {
	TotalRequests int     `json:"totalRequests"`
	TotalAmount   float64 `json:"totalAmount"`
}
