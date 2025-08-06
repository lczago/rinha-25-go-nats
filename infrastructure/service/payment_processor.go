package service

import (
	"bytes"
	"fmt"

	"github.com/goccy/go-json"

	"net/http"
)

type processor struct {
	health      bool
	minRespTime int
	url         string
	orderType   ProcessorType
}

func (p *processor) processWithClient(input PostPaymentProcessor) error {
	body, _ := json.Marshal(input)
	req, err := http.NewRequest(http.MethodPost, fmt.Sprintf("%s/payments", p.url), bytes.NewBuffer(body))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusUnprocessableEntity {
		return ErrUnprocessableEntity
	} else if resp.StatusCode == http.StatusInternalServerError {
		return ErrInternalServerError
	}

	return nil
}
