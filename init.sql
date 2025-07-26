CREATE TABLE payments
(
    correlation_id UUID  PRIMARY KEY      NOT NULL,
    amount         DECIMAL     NOT NULL,
    processed_at   TIMESTAMPTZ   NOT NULL,
    processed_by   VARCHAR(15) NOT NULL
);

CREATE INDEX idx_payments_processed_at ON payments(processed_at);
CREATE INDEX idx_payments_processed_by ON payments(processed_by);

