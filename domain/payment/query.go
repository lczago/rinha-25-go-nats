package payment

import "time"

type SummaryDate struct {
	From *time.Time `query:"from"`
	To   *time.Time `query:"to"`
}
