package eventstream

import (
	"context"
	"errors"
	"io"
	"strings"
	"testing"
	"time"

	"github.com/Bennybl/session-handler/internal/service"
	"github.com/Bennybl/session-handler/internal/session"
)

func TestStdinSourceReadsNDJSONAndEndsCleanly(t *testing.T) {
	t.Parallel()
	input := string(validEventJSON()) + "\n\n" + string(validEventJSON()) + "\n"
	source, err := NewStdinSource(strings.NewReader(input))
	if err != nil {
		t.Fatalf("NewStdinSource() error = %v", err)
	}
	for index := range 2 {
		message, err := source.Next(context.Background())
		if err != nil {
			t.Fatalf("Next(%d) error = %v", index, err)
		}
		event, err := DecodeEvent(message.Data())
		if err != nil {
			t.Fatalf("DecodeEvent(%d) error = %v", index, err)
		}
		if event.Type != service.EventLogin || event.EventID != "90000000-0000-4000-8000-000000000001" {
			t.Fatalf("event %d = %+v", index, event)
		}
	}
	if _, err := source.Next(context.Background()); !errors.Is(err, io.EOF) {
		t.Fatalf("final Next() error = %v, want EOF", err)
	}
	if err := source.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}
}

func TestDecodeEventRejectsMalformedAndUnknownJSON(t *testing.T) {
	t.Parallel()
	values := [][]byte{
		[]byte(`{"eventId":`),
		[]byte(`{"eventId":"90000000-0000-4000-8000-000000000001","type":"LOGIN","unknown":true}`),
		append(validEventJSON(), []byte(` {}`)...),
	}
	for _, value := range values {
		if _, err := DecodeEvent(value); !errors.Is(err, session.ErrInvalidInput) {
			t.Fatalf("DecodeEvent(%q) error = %v, want ErrInvalidInput", value, err)
		}
	}
}

func TestStdinSourceHonorsPreCancelledContext(t *testing.T) {
	t.Parallel()
	source, err := NewStdinSource(strings.NewReader(string(validEventJSON())))
	if err != nil {
		t.Fatalf("NewStdinSource() error = %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := source.Next(ctx); !errors.Is(err, context.Canceled) {
		t.Fatalf("Next() error = %v, want context cancellation", err)
	}
}

func TestStdinSourceCancellationInterruptsClosableReader(t *testing.T) {
	t.Parallel()
	reader, writer := io.Pipe()
	t.Cleanup(func() { _ = writer.Close() })
	source, err := NewStdinSource(reader)
	if err != nil {
		t.Fatalf("NewStdinSource() error = %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	result := make(chan error, 1)
	go func() {
		_, err := source.Next(ctx)
		result <- err
	}()
	cancel()
	select {
	case err := <-result:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("Next() error = %v, want context cancellation", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Next() did not stop after cancellation")
	}
}
