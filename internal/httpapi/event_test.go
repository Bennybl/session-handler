package httpapi

import (
	"bytes"
	"context"
	"errors"
	"log"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/Bennybl/session-handler/internal/service"
	"github.com/Bennybl/session-handler/internal/session"
)

type submitterFunc func(context.Context, service.Event) error

func (fn submitterFunc) Submit(ctx context.Context, event service.Event) error { return fn(ctx, event) }

func TestEventIngressStrictlyDecodesNormalizesAndSubmits(t *testing.T) {
	var submitted service.Event
	var logs bytes.Buffer
	handler, err := NewEventHandler(submitterFunc(func(_ context.Context, event service.Event) error { submitted = event; return nil }), log.New(&logs, "", 0))
	if err != nil {
		t.Fatal(err)
	}
	body := `{"eventId":"E0000000-0000-4000-8000-000000000001","type":" login ","tenantId":"tenant-a","username":"alice","ip":"2001:0db8::1","tags":["user"],"timestamp":"2026-08-21T10:00:00+03:00"}`
	response := serveEvent(handler, body)
	if response.Code != http.StatusNoContent {
		t.Fatalf("status=%d body=%s", response.Code, response.Body.String())
	}
	if submitted.Key.IP != "2001:db8::1" || submitted.Type != service.EventLogin || submitted.EventID[0] != 'e' || submitted.Timestamp.Location().String() != "UTC" {
		t.Fatalf("submitted = %+v", submitted)
	}
	if logs.Len() != 0 {
		t.Fatalf("unexpected dead letter: %s", logs.String())
	}
}

func TestEventIngressRejectsMalformedAndMapsTerminalFailures(t *testing.T) {
	for _, test := range []struct {
		name, body  string
		submitError error
		want        int
	}{
		{name: "unknown field", body: `{"unknown":true}`, want: http.StatusBadRequest},
		{name: "trailing JSON", body: `{} {}`, want: http.StatusBadRequest},
		{name: "invalid transition", body: validEventJSON(), submitError: session.ErrInvalidTransition, want: http.StatusConflict},
		{name: "transient exhausted", body: validEventJSON(), submitError: errors.New("database unavailable"), want: http.StatusServiceUnavailable},
	} {
		t.Run(test.name, func(t *testing.T) {
			var logs bytes.Buffer
			handler, err := NewEventHandler(submitterFunc(func(context.Context, service.Event) error { return test.submitError }), log.New(&logs, "", 0))
			if err != nil {
				t.Fatal(err)
			}
			response := serveEvent(handler, test.body)
			if response.Code != test.want {
				t.Fatalf("status=%d body=%s", response.Code, response.Body.String())
			}
			if test.want != http.StatusServiceUnavailable && !strings.Contains(logs.String(), `"kind":"dead_letter"`) {
				t.Fatalf("missing dead letter: %s", logs.String())
			}
		})
	}
}

func validEventJSON() string {
	return `{"eventId":"e0000000-0000-4000-8000-000000000001","type":"LOGIN","tenantId":"tenant-a","username":"alice","ip":"192.0.2.10","timestamp":"2026-08-21T10:00:00Z"}`
}
func serveEvent(handler http.Handler, body string) *httptest.ResponseRecorder {
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, httptest.NewRequest(http.MethodPost, "/v1/events", strings.NewReader(body)))
	return response
}
