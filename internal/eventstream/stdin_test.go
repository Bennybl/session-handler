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

// The stdin source reads newline-delimited JSON, skips blank lines, and reports
// a clean end of input rather than an error.
func TestStdinSourceReadsNDJSONAndEndsCleanly(t *testing.T) {
	t.Parallel()

	line := string(validEventJSON())
	source, err := NewStdinSource(strings.NewReader(line + "\n\n" + line + "\n"))
	if err != nil {
		t.Fatalf("NewStdinSource() error = %v", err)
	}

	for index := range 2 {
		message, err := source.Next(context.Background())
		if err != nil {
			t.Fatalf("Next() error at message %d = %v", index, err)
		}
		event, err := DecodeEvent(message.Data())
		if err != nil {
			t.Fatalf("DecodeEvent() error at message %d = %v", index, err)
		}
		if event.Type != service.EventLogin || event.EventID != streamEventID {
			t.Fatalf("message %d = %+v, want a login carrying %q", index, event, streamEventID)
		}
	}

	if _, err := source.Next(context.Background()); !errors.Is(err, io.EOF) {
		t.Errorf("Next() after the last line = %v, want io.EOF", err)
	}
	if err := source.Close(); err != nil {
		t.Errorf("Close() error = %v", err)
	}
}

func TestDecodeEventRejectsMalformedAndUnknownJSON(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		data []byte
	}{
		{name: "truncated JSON", data: []byte(`{"eventId":`)},
		{name: "unknown field", data: []byte(`{"eventId":"` + streamEventID + `","type":"LOGIN","unknown":true}`)},
		{name: "trailing value", data: append(validEventJSON(), []byte(` {}`)...)},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			if _, err := DecodeEvent(test.data); !errors.Is(err, session.ErrInvalidInput) {
				t.Fatalf("DecodeEvent(%q) error = %v, want ErrInvalidInput", test.data, err)
			}
		})
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
		t.Fatalf("Next() error = %v, want context.Canceled", err)
	}
}

// A read already blocked on an idle pipe must still stop when the context ends,
// so shutdown does not wait for input that never arrives.
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
			t.Fatalf("Next() error = %v, want context.Canceled", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Next() did not return after cancellation")
	}
}
