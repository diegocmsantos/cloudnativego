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

func (l *PostgresTransactionLogger) verifyTableExists() (bool, error) {
	const table = "transactions"

	var result string

	rows, err := l.db.Query(fmt.Sprintf("SELECT to_regclass('public.%s');", table))
	defer rows.Close()
	if err != nil {
		return false, err
	}

	for rows.Next() && result != table {
		rows.Scan(&result)
	}

	return result == table, rows.Err()
}

func (l *PostgresTransactionLogger) createTable() error {
	_, err := l.db.Exec(`
	CREATE TABLE transactions (
		sequence      BIGSERIAL PRIMARY KEY,
		event_type    SMALLINT,
		key 		  TEXT,
		value         TEXT
	);`)
	return err
}

func (l *PostgresTransactionLogger) Run() {
	events := make(chan Event, 16)
	l.events = events
	errs := make(chan error, 1)
	l.errs = errs

	go func() {
		query := `
			INSERT INTO transaction
			(event_type, key, value)
			VALUES ($1, $2, $3);
		`

		for e := range events {
			_, err := l.db.Exec(query, e.EventType, e.Key, e.Value)
			if err != nil {
				errs <- err
				return
			}
		}
	}()
}

func (l *PostgresTransactionLogger) ReadEvents() (<-chan Event, <-chan error) {
	outEvent := make(chan Event)
	outError := make(chan error, 1)

	query := "select event_type, key, value from transaction"

	go func() {
		defer close(outEvent)
		defer close(outError)

		rows, err := l.db.Query(query)
		if err != nil {
			outError <- fmt.Errorf("sql query error: %v", err)
			return
		}

		defer rows.Close()

		var e Event

		for rows.Next() {
			err := rows.Scan(&e.EventType, &e.Key, &e.Value)
			if err != nil {
				outError <- err
				return
			}

			outEvent <- e
		}

		err = rows.Err()
		if err != nil {
			outError <- fmt.Errorf("transaction log read failure: %v", err)
		}
	}()

	return outEvent, outError
}

func (l *PostgresTransactionLogger) Close() error {
	if l.events != nil {
		close(l.events) // Terminates Run loop and goroutine
	}

	return l.db.Close()
}
