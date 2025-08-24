package db

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/lightsparkdev/spark/common/logging"
	"github.com/lightsparkdev/spark/so/ent"
	soerrors "github.com/lightsparkdev/spark/so/errors"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/trace"
)

var (
	ErrTxBeginTimeout   = soerrors.UnavailableErrorf("The service is currently unavailable. Please try again later.")
	DefaultNewTxTimeout = 15 * time.Second
)

var (
	// Metrics
	txDurationHistogram metric.Float64Histogram
	txCounter           metric.Int64Counter
	txActiveGauge       metric.Int64UpDownCounter

	// Common attribute values
	attrOperationCommit   = attribute.String("operation", "commit")
	attrOperationRollback = attribute.String("operation", "rollback")
	attrOperationBegin    = attribute.String("operation", "begin")
	attrStatusSuccess     = attribute.String("status", "success")
	attrStatusError       = attribute.String("status", "error")

	// Initialize metrics
	_ = initMetrics()
)

func initMetrics() error {
	meter := otel.GetMeterProvider().Meter("spark.db")

	var err error
	txDurationHistogram, err = meter.Float64Histogram(
		"transaction_duration_ms",
		metric.WithDescription("Database transaction duration in milliseconds"),
		metric.WithUnit("ms"),
		metric.WithExplicitBucketBoundaries(
			0.001, 0.005, 0.01, 0.05, 0.1, 0.5, 1,
			5, 10, 25, 50, 100, 250, 500,
			1000, 2500, 5000, 10000, 25000, 50000, 100000,
		),
	)
	if err != nil {
		return err
	}

	txCounter, err = meter.Int64Counter(
		"transactions_total",
		metric.WithDescription("Total number of database transactions"),
	)
	if err != nil {
		return err
	}

	txActiveGauge, err = meter.Int64UpDownCounter(
		"transactions_active",
		metric.WithDescription("Number of currently active database transactions"),
	)
	if err != nil {
		return err
	}

	return nil
}

// Context keys for storing RPC method and task name
type dbSessionAttrContextKey string

const (
	dbSessionAttrKey dbSessionAttrContextKey = "db_session_attr"
)

// getMetricAttributes returns the attributes for metrics including RPC method and task name
func getMetricAttributes(ctx context.Context) []attribute.KeyValue {
	if metricAttrs, ok := ctx.Value(dbSessionAttrKey).([]attribute.KeyValue); ok {
		return metricAttrs
	}

	return nil
}

// getGaugeAttributes returns the attributes for gauge operations
func getGaugeAttributes(ctx context.Context, operationAttr attribute.KeyValue) []attribute.KeyValue {
	return append([]attribute.KeyValue{operationAttr}, getMetricAttributes(ctx)...)
}

// getOperationAttributes returns the attributes for a specific operation
func getOperationAttributes(ctx context.Context, operationAttr attribute.KeyValue, statusAttr attribute.KeyValue) []attribute.KeyValue {
	attrs := []attribute.KeyValue{operationAttr, statusAttr}
	attrs = append(attrs, getMetricAttributes(ctx)...)
	return attrs
}

// addTraceEvent adds a trace event if a span is available
func addTraceEvent(ctx context.Context, operation string, duration float64, err error) {
	span := trace.SpanFromContext(ctx)
	if span != nil {
		eventName := "db.transaction." + operation
		span.AddEvent(eventName, trace.WithAttributes(
			getTraceAttributes(operation, duration, err)...,
		))
	}
}

// getTraceAttributes returns the attributes for trace events
// operation: the operation type (begin, commit, rollback)
// duration: duration in seconds (0 for operations without duration)
// err: error if the operation failed - optional (status is inferred from this)
func getTraceAttributes(operation string, duration float64, err error) []attribute.KeyValue {
	attrs := []attribute.KeyValue{
		attribute.String("db.transaction.operation", operation),
	}
	if duration > 0 {
		attrs = append(attrs, attribute.Float64("db.transaction.duration_seconds", duration))
	}

	if err != nil {
		attrs = append(attrs, attrStatusError, attribute.String("error", err.Error()))
	}
	attrs = append(attrs, attrStatusSuccess)

	return attrs
}

func WithMetricAttributes(ctx context.Context, metricAttr []attribute.KeyValue) context.Context {
	return context.WithValue(ctx, dbSessionAttrKey, metricAttr)
}

// SessionFactory is an interface for creating a new Session.
type SessionFactory interface {
	NewSession(ctx context.Context) *Session
}

// DefaultSessionFactory is the default implementation of SessionFactory that creates sessions
// using an ent.Client. It also provides a timeout for how long it will wait for a new transaction
// to be started, to prevent requests from hanging indefinitely if the database is unresponsive or
// overloaded.
type DefaultSessionFactory struct {
	dbClient     *ent.Client
	newTxTimeout *time.Duration
}

func NewDefaultSessionFactory(dbClient *ent.Client, newTxTimeout *time.Duration) *DefaultSessionFactory {
	return &DefaultSessionFactory{
		dbClient:     dbClient,
		newTxTimeout: newTxTimeout,
	}
}

func (f *DefaultSessionFactory) NewSession(ctx context.Context) *Session {
	provider := NewTxProviderWithTimeout(ent.NewEntClientTxProvider(f.dbClient), f.newTxTimeout)

	return &Session{
		ctx:       ctx,
		provider:  provider,
		currentTx: nil,
		mu:        sync.Mutex{},
	}
}

// A Session manages a transaction over the lifetime of a request or worker. It
// wraps a TxProvider for creating an initial transaction, and stores that transaction for
// subsequent requests until the transaction is committed or rolled back. Once the transaction
// is finished, it is cleared so a new one can begin the next time `GetOrBeginTx` is called.
type Session struct {
	// ctx is the context for this session. It is used to for creating new transactions within the
	// session to ensure that the session can clean those transactions up even if the context in which
	// the caller is operating is cancelled.
	ctx context.Context
	// TxProvider is used to create a new transaction when needed.
	provider ent.TxProvider
	// Mutex for ensuring thread-safe access to `currentTx` and `timer`.
	mu sync.Mutex
	// The current transaction being tracked by this session if a transaction has been started. When
	// the tracked transaction is committed or rolled back successfully, this field is set back to nil.
	currentTx *ent.Tx
	// A timer for tracking how long a transaction is held. If a transaction is held for more than
	// a set amount of time, we log a warning. This is useful for tracking down long running
	// transactions.
	timer *time.Timer
	// startTime is the time when the current transaction was started.
	startTime time.Time
}

// NewSession creates a new Session with a new transactions provided
// by an ent.ClientTxProvider wrapping the provided `ent.Client`.
func NewSession(ctx context.Context, dbClient *ent.Client, newTxTimeout *time.Duration) *Session {
	provider := NewTxProviderWithTimeout(ent.NewEntClientTxProvider(dbClient), newTxTimeout)

	return &Session{
		ctx:       ctx,
		provider:  provider,
		currentTx: nil,
		mu:        sync.Mutex{},
	}
}

// GetOrBeginTx retrieves the current transaction if it exists, otherwise it begins a new one.
// Furthermore, it inserts commit and rollback hooks that will clear the current transaction
// should the transaction be finished by the caller.
func (s *Session) GetOrBeginTx(ctx context.Context) (*ent.Tx, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.currentTx == nil {
		logger := logging.GetLoggerFromContext(ctx)
		logger.Info("Creating new transaction")

		txActiveGauge.Add(ctx, 1, metric.WithAttributes(getGaugeAttributes(ctx, attrOperationBegin)...))

		// Important! We need to use the context from the session, not the one passed in, because we want
		// to ensure the transaction can be cleaned up even if the context passed in is cancelled.
		tx, err := s.provider.GetOrBeginTx(s.ctx)
		if err != nil {
			logger.Error("Failed to create new transaction", "error", err)
			// Decrement on error
			txActiveGauge.Add(ctx, -1, metric.WithAttributes(getGaugeAttributes(ctx, attrOperationBegin)...))

			addTraceEvent(ctx, "begin", 0, err)

			return nil, err
		}
		logger.Info("New transaction created")
		s.currentTx = tx
		s.startTime = time.Now()
		s.timer = time.AfterFunc(30*time.Second, func() {
			logger.Info("Transaction is held for more than 30 seconds")
		})

		addTraceEvent(ctx, "begin", 0, nil)

		tx.OnCommit(func(fn ent.Committer) ent.Committer {
			return ent.CommitFunc(func(ctx context.Context, tx *ent.Tx) error {
				s.mu.Lock()
				defer s.mu.Unlock()
				logger.Info("Committing transaction")

				duration := time.Since(s.startTime).Seconds()
				durationMs := duration * 1000

				err := fn.Commit(ctx, tx)
				var attrs []attribute.KeyValue
				if err != nil {
					logger.Error("Failed to commit transaction", "error", err)
					attrs = getOperationAttributes(ctx, attrOperationCommit, attrStatusError)
					addTraceEvent(ctx, "commit", duration, err)
				} else {
					// Only set the current tx to nil if the transaction was committed successfully.
					// Otherwise, the transaction will be rolled back at last.
					attrs = getOperationAttributes(ctx, attrOperationCommit, attrStatusSuccess)
					txDurationHistogram.Record(ctx, durationMs, metric.WithAttributes(attrs...))
					txActiveGauge.Add(ctx, -1, metric.WithAttributes(getGaugeAttributes(ctx, attrOperationCommit)...))

					addTraceEvent(ctx, "commit", duration, nil)
				}

				if err == nil || errors.Is(err, sql.ErrTxDone) || errors.Is(err, context.Canceled) {
					s.timer.Stop()
					s.timer = nil
					s.currentTx = nil
				}

				txCounter.Add(ctx, 1, metric.WithAttributes(attrs...))

				return err
			})
		})
		tx.OnRollback(func(fn ent.Rollbacker) ent.Rollbacker {
			return ent.RollbackFunc(func(ctx context.Context, tx *ent.Tx) error {
				s.mu.Lock()
				defer s.mu.Unlock()
				logger.Info("Rolling back transaction")

				duration := time.Since(s.startTime).Seconds()

				err := fn.Rollback(ctx, tx)
				var attrs []attribute.KeyValue
				if err != nil {
					logger.Error("Failed to rollback transaction", "error", err)
					attrs = getOperationAttributes(ctx, attrOperationRollback, attrStatusError)
					addTraceEvent(ctx, "rollback", duration, err)
				} else {
					attrs = getOperationAttributes(ctx, attrOperationRollback, attrStatusSuccess)
					addTraceEvent(ctx, "rollback", duration, nil)
				}

				txDurationHistogram.Record(ctx, duration, metric.WithAttributes(attrs...))
				txCounter.Add(ctx, 1, metric.WithAttributes(attrs...))
				txActiveGauge.Add(ctx, -1, metric.WithAttributes(getGaugeAttributes(ctx, attrOperationRollback)...))
				s.timer.Stop()
				s.timer = nil
				s.currentTx = nil
				return err
			})
		})
	}
	return s.currentTx, nil
}

// GetTxIfExists retrieves the current transaction if it exists, without starting a new one. If
// no current transaction exists, then returns nil.
func (s *Session) GetTxIfExists() *ent.Tx {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.currentTx
}

// A wrapper around a TxProvider that includes a timeout for if it takes to long to call `GetOrBeginTx`.
type TxProviderWithTimeout struct {
	wrapped ent.TxProvider
	timeout time.Duration
}

func NewTxProviderWithTimeout(provider ent.TxProvider, timeout *time.Duration) *TxProviderWithTimeout {
	if timeout == nil {
		timeout = &DefaultNewTxTimeout
	}

	return &TxProviderWithTimeout{
		wrapped: provider,
		timeout: *timeout,
	}
}

func (t *TxProviderWithTimeout) GetOrBeginTx(ctx context.Context) (*ent.Tx, error) {
	if t.timeout <= 0 {
		// If the timeout is zero or negative, assume there is no timeout and we should call
		// `GetOrBeginTx` normally.
		return t.wrapped.GetOrBeginTx(ctx)
	}

	txChan := make(chan *ent.Tx)
	errChan := make(chan error)

	logger := logging.GetLoggerFromContext(ctx)

	timeoutCtx, cancel := context.WithTimeout(ctx, t.timeout)
	defer cancel()

	// We can't pass timeoutCtx directly into `NewTx`, because the context we pass into `NewTx` isn't
	// just for starting the transaction. It's also used for the lifetime of the transaction. So throw
	// this into a goroutine so that we can run a select on:
	//
	// 		1. A connection being acquired and a transaction being started successfully.
	//		2. An error occurring while starting the transaction.
	//		3. Neither (1) nor (2) happening within the timeout period.
	//
	// If (3) happens, this function will return an error AND the caller needs to cancel the context in
	// order to stop the transaction process.
	go func() {
		defer close(txChan)
		defer close(errChan)

		tx, err := t.wrapped.GetOrBeginTx(ctx)
		if err != nil {
			select {
			case errChan <- err:
			case <-timeoutCtx.Done():
				logger.Warn("Failed to start transaction within timeout", "error", err)
				return
			}
			return
		}

		select {
		case txChan <- tx:
		case <-timeoutCtx.Done():
			// If the timeout context is done, there are no receivers for the transaction, so we need to
			// rollback the transaction so that we aren't just leaving it idle.
			err := tx.Rollback()
			if err != nil {
				logger.Warn("Failed to rollback transaction after timeout", "error", err)
			}
			return
		}
	}()

	select {
	case tx := <-txChan:
		return tx, nil
	case err := <-errChan:
		return nil, fmt.Errorf("failed to start transaction: %w", err)
	case <-timeoutCtx.Done():
		return nil, ErrTxBeginTimeout
	}
}
