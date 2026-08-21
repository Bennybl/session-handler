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
// a clean end of input. An envelope that is malformed, carries an unknown field,
// or has trailing content is refused rather than half-decoded.
func TestStdinSourceReadsNDJSONAndRejectsMalformedEnvelopes(t *testing.T) {
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

	malformed := []struct {
		name string
		data []byte
	}{
		{name: "truncated JSON", data: []byte(`{"eventId":`)},
		{name: "unknown field", data: []byte(`{"eventId":"` + streamEventID + `","type":"LOGIN","unknown":true}`)},
		{name: "trailing value", data: append(validEventJSON(), []byte(` {}`)...)},
	}
	for _, test := range malformed {
		if _, err := DecodeEvent(test.data); !errors.Is(err, session.ErrInvalidInput) {
			t.Errorf("%s: DecodeEvent() error = %v, want ErrInvalidInput", test.name, err)
		}
	}
}

// Cancellation stops the source whether it arrives before the read or while the
// read is already blocked on an idle pipe, so shutdown never waits for input
// that will not come.
func TestStdinSourceHonorsCancellation(t *testing.T) {
	t.Parallel()

	source, err := NewStdinSource(strings.NewReader(string(validEventJSON())))
	if err != nil {
		t.Fatalf("NewStdinSource() error = %v", err)
	}
	cancelled, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := source.Next(cancelled); !errors.Is(err, context.Canceled) {
		t.Errorf("Next() with a cancelled context = %v, want context.Canceled", err)
	}

	reader, writer := io.Pipe()
	t.Cleanup(func() { _ = writer.Close() })
	blocked, err := NewStdinSource(reader)
	if err != nil {
		t.Fatalf("NewStdinSource() error = %v", err)
	}
	ctx, stop := context.WithCancel(context.Background())
	result := make(chan error, 1)
	go func() {
		_, err := blocked.Next(ctx)
		result <- err
	}()
	stop()

	select {
	case err := <-result:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("Next() error = %v, want context.Canceled", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Next() did not return after cancellation")
	}
}
