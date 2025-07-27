package payment

import (
	"context"
	"fmt"
	"github.com/redis/go-redis/v9"
	"rinha-25-go-nats/infrastructure/service"
	"strconv"
	"time"
)

var processors = []string{
	string(service.ProcessorTypeDefault),
	string(service.ProcessorTypeFallback),
}

type IRepository interface {
	Insert(ctx context.Context, payment Entity) error
	AggregateSummary(ctx context.Context, summaryDate SummaryDate) (*ProcessorsSummary, error)
	DeleteAll(ctx context.Context) error
}

type repository struct {
	client *redis.Client
}

func NewRepository(client *redis.Client) IRepository {
	return &repository{client}
}

func summaryDataKey(processor string) string {
	return fmt.Sprintf("summary:%s:data", processor)
}
func summaryHistoryKey(processor string) string {
	return fmt.Sprintf("summary:%s:history", processor)
}

func (r *repository) Insert(ctx context.Context, payment Entity) error {
	var (
		dataKey    = summaryDataKey(payment.ProcessedBy)
		historyKey = summaryHistoryKey(payment.ProcessedBy)
		id         = payment.CorrelationId
		amount     = payment.Amount
		timestamp  = float64(payment.ProcessedAt.UnixMilli())
	)

	pipe := r.client.TxPipeline()
	pipe.HSet(ctx, dataKey, id, amount)
	pipe.ZAdd(ctx, historyKey, redis.Z{Score: timestamp, Member: id})

	_, err := pipe.Exec(ctx)
	return err
}

func (r *repository) AggregateSummary(ctx context.Context, summaryDate SummaryDate) (*ProcessorsSummary, error) {
	result := map[string]Summary{}

	for _, proc := range processors {
		var (
			historyKey = summaryHistoryKey(proc)
			dataKey    = summaryDataKey(proc)
			from       = int64(0)
			to         = time.Now().UTC().UnixMilli()
		)

		if summaryDate.From != nil {
			from = summaryDate.From.UnixMilli()
		}
		if summaryDate.To != nil {
			to = summaryDate.To.UnixMilli()
		}

		ids, err := r.client.ZRangeByScore(ctx, historyKey, &redis.ZRangeBy{
			Min: fmt.Sprintf("%d", from),
			Max: fmt.Sprintf("%d", to),
		}).Result()
		if err != nil {
			return nil, err
		}

		totalRequests := len(ids)
		totalAmount := 0.0

		if totalRequests > 0 {
			amounts, err := r.client.HMGet(ctx, dataKey, ids...).Result()
			if err != nil {
				return nil, err
			}
			for _, a := range amounts {
				if a == nil {
					continue
				}
				value, canCast := a.(string)
				if !canCast {
					return nil, fmt.Errorf("invalid type for amount")
				}

				f, err := strconv.ParseFloat(value, 64)
				if err != nil {
					return nil, fmt.Errorf("invalid float value: %v", err)
				}
				totalAmount += f
			}
		}
		result[proc] = Summary{
			TotalRequests: totalRequests,
			TotalAmount:   totalAmount,
		}
	}

	return &ProcessorsSummary{
		Default:  result["DEFAULT"],
		FallBack: result["FALLBACK"],
	}, nil
}

func (r *repository) DeleteAll(ctx context.Context) error {
	return r.client.Del(
		ctx,
		summaryDataKey(string(service.ProcessorTypeDefault)),
		summaryHistoryKey(string(service.ProcessorTypeDefault)),
	).Err()
}
