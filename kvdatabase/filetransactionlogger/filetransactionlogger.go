package filetransactionlogger

import (
	"fmt"
	"os"

	"github.com/diegocmsantos/kvdatabase/kvdatabase"
)

type FileTransactionLogger struct {
	events       chan<- kvdatabase.Event
	errors       <-chan error
	lastSequence uint64
	file         *os.File
}

func NewFileTransactionLogger(filename string) (kvdatabase.TransactionLogger, error) {
	file, err := os.OpenFile(filename, os.O_RDWR|os.O_APPEND|os.O_CREATE, 0755)
	if err != nil {
		return nil, err
	}
	return &FileTransactionLogger{
		file: file,
	}, nil
}

func (l *FileTransactionLogger) Run() {
	events := make(chan kvdatabase.Event, 16)
	l.events = events

	errors := make(chan error)
	l.errors = errors

	go func() {
		for e := range events {
			l.lastSequence++

			_, err := fmt.Fprintf(
				l.file,
				"%d\t%d\t%s\t%s\n",
				l.lastSequence, e.EventType, e.Key, e.Value)
			if err != nil {
				errors <- err
			}
		}
	}()
}

func (f *FileTransactionLogger) WritePut(key, value string) {
	f.events <- kvdatabase.Event{EventType: kvdatabase.EventPut, Key: key, Value: value}
}

func (f *FileTransactionLogger) WriteDelete(key string) {
	f.events <- kvdatabase.Event{EventType: kvdatabase.EventDelete, Key: key}
}

func (f *FileTransactionLogger) Err() <-chan error {
	return f.errors
}
