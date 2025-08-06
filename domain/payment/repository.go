package payment

import (
	"context"
	"fmt"
	"rinha-25-go-nats/infrastructure/service"
	"strconv"
	"sync"
	"time"

	"github.com/redis/go-redis/v9"
)

var processors = []string{
	string(service.ProcessorTypeDefault),
	string(service.ProcessorTypeFallback),
}

var (
	defaultDataKey     = fmt.Sprintf("summary:%s:data", string(service.ProcessorTypeDefault))
	defaultHistoryKey  = fmt.Sprintf("summary:%s:history", string(service.ProcessorTypeDefault))
	fallbackDataKey    = fmt.Sprintf("summary:%s:data", string(service.ProcessorTypeFallback))
	fallbackHistoryKey = fmt.Sprintf("summary:%s:history", string(service.ProcessorTypeFallback))
)

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

func getDataKey(processor string) string {
	if processor == string(service.ProcessorTypeDefault) {
		return defaultDataKey
	}
	return fallbackDataKey
}

func getHistoryKey(processor string) string {
	if processor == string(service.ProcessorTypeDefault) {
		return defaultHistoryKey
	}
	return fallbackHistoryKey
}

func (r *repository) Insert(ctx context.Context, payment Entity) error {
	var (
		dataKey    = getDataKey(payment.ProcessedBy)
		historyKey = getHistoryKey(payment.ProcessedBy)
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
	result := make([]Summary, len(processors))
	var wg sync.WaitGroup

	for idx, proc := range processors {
		wg.Add(1)
		go func(idx int, processor string) {
			defer wg.Done()

			summary, err := r.aggregateForProcessor(ctx, processor, summaryDate)
			if err != nil {
				return
			}

			result[idx] = summary
		}(idx, proc)
	}

	wg.Wait()

	return &ProcessorsSummary{
		Default:  result[0],
		FallBack: result[1],
	}, nil
}

func (r *repository) aggregateForProcessor(ctx context.Context, processor string, summaryDate SummaryDate) (Summary, error) {
	var (
		historyKey = getHistoryKey(processor)
		dataKey    = getDataKey(processor)
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
		return Summary{}, err
	}

	totalRequests := len(ids)
	totalAmount := 0.0

	if totalRequests > 0 {
		amounts, err := r.client.HMGet(ctx, dataKey, ids...).Result()
		if err != nil {
			return Summary{}, err
		}

		for _, a := range amounts {
			if a == nil {
				continue
			}
			value, canCast := a.(string)
			if !canCast {
				return Summary{}, fmt.Errorf("invalid type for amount")
			}

			f, err := strconv.ParseFloat(value, 64)
			if err != nil {
				return Summary{}, fmt.Errorf("invalid float value: %v", err)
			}
			totalAmount += f
		}
	}

	return Summary{
		TotalRequests: totalRequests,
		TotalAmount:   totalAmount,
	}, nil
}

func (r *repository) DeleteAll(ctx context.Context) error {
	return r.client.Del(
		ctx,
		defaultDataKey,
		defaultHistoryKey,
		fallbackDataKey,
		fallbackHistoryKey,
	).Err()
}
