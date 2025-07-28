package payment

import (
	"context"
	"database/sql"
	"fmt"
)

type IRepository interface {
	Insert(ctx context.Context, payment Entity) error
	AggregateSummary(ctx context.Context, summaryDate SummaryDate) (*ProcessorsSummary, error)
	DeleteAll(ctx context.Context) error
}

type repository struct {
	db *sql.DB
}

func NewRepository(db *sql.DB) IRepository {
	return &repository{db}
}

func (r *repository) Insert(ctx context.Context, payment Entity) error {
	query := `
		 INSERT INTO payments (correlation_id, amount, processed_by, processed_at)
    	 VALUES ($1, $2, $3, $4)
	`
	_, err := r.db.ExecContext(
		ctx,
		query,
		payment.CorrelationId,
		payment.Amount,
		payment.ProcessedBy,
		payment.ProcessedAt,
	)
	return err
}

func (r *repository) AggregateSummary(ctx context.Context, summaryDate SummaryDate) (*ProcessorsSummary, error) {
	query, args := buildSummaryQuery(summaryDate)
	rows, err := r.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	result := map[string]Summary{}

	for rows.Next() {
		var processedBy string
		var totalRequests int
		var totalAmount float64

		if err = rows.Scan(&processedBy, &totalRequests, &totalAmount); err != nil {
			return nil, err
		}
		result[processedBy] = Summary{
			TotalRequests: totalRequests,
			TotalAmount:   totalAmount,
		}
	}

	out := &ProcessorsSummary{
		Default:  result["DEFAULT"],
		FallBack: result["FALLBACK"],
	}
	return out, nil
}

func buildSummaryQuery(summaryDate SummaryDate) (string, []interface{}) {
	base := `
		SELECT processed_by, COUNT(*) as total_requests, COALESCE(SUM(amount), 0) as total_amount
		FROM payments
	`
	clauses := []string{}
	args := []any{}
	paramIdx := 1

	if summaryDate.From != nil {
		clauses = append(clauses, fmt.Sprintf("processed_at >= $%d", paramIdx))
		args = append(args, *summaryDate.From)
		paramIdx++
	}
	if summaryDate.To != nil {
		clauses = append(clauses, fmt.Sprintf("processed_at < $%d", paramIdx))
		args = append(args, *summaryDate.To)
		paramIdx++
	}

	if len(clauses) > 0 {
		base += " WHERE " + joinClauses(clauses, " AND ")
	}
	base += " GROUP BY processed_by"
	return base, args
}

func joinClauses(clauses []string, sep string) string {
	out := ""
	for i, c := range clauses {
		if i > 0 {
			out += sep
		}
		out += c
	}
	return out
}

func (r *repository) DeleteAll(ctx context.Context) error {
	query := `
		 DELETE FROM payments
	`
	_, err := r.db.ExecContext(ctx, query)
	return err
}
