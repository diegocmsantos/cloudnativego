package kvdatabase

import (
	"database/sql"
	"fmt"

	_ "github.com/lib/pq"
)

type PostgresTransactionLogger struct {
	events chan<- Event
	errs   <-chan error
	db     *sql.DB
}

type PostgresDBParams struct {
	dbName   string
	host     string
	username string
	password string
}

func NewPostgresTransactionLogger(opts PostgresDBParams) (TransactionLogger, error) {
	connStr := fmt.Sprintf("host=%s dname=%s user=%s password=%s",
		opts.host, opts.dbName, opts.username, opts.password)

	db, err := sql.Open("postgres", connStr)
	if err != nil {
		return nil, fmt.Errorf("failed to open db: %w", err)
	}

	if err := db.Ping(); err != nil {
		return nil, fmt.Errorf("failed to connect to db: %w", err)
	}

	logger := &PostgresTransactionLogger{db: db}

	exists, err := logger.verifyTableExists()
	if err != nil {
		return nil, fmt.Errorf("failed to verify the table existence: %w", err)
	}

	if !exists {
		if err := logger.createTable(); err != nil {
			return nil, fmt.Errorf("failed to create table: %w", err)
		}
	}

	return logger, nil
}

func (l *PostgresTransactionLogger) WritePut(key, value string) {
	l.events <- Event{EventType: EventPut, Key: key, Value: value}
}

func (l *PostgresTransactionLogger) WriteDelete(key string) {
	l.events <- Event{EventType: EventDelete, Key: key}
}

func (l *PostgresTransactionLogger) Err() <-chan error {
	return l.errs
}
