package httpapi

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"time"

	"github.com/Bennybl/session-handler/internal/eventstream"
	"github.com/Bennybl/session-handler/internal/service"
	"github.com/Bennybl/session-handler/internal/session"
)

const maximumEventBytes = 1024 * 1024

type EventSubmitter interface {
	Submit(context.Context, service.Event) error
}

type eventHandler struct {
	submitter EventSubmitter
	logger    *log.Logger
	now       func() time.Time
}

type eventRequest struct {
	EventID   string            `json:"eventId"`
	Type      service.EventType `json:"type"`
	TenantID  string            `json:"tenantId"`
	Username  string            `json:"username"`
	IP        string            `json:"ip"`
	Tags      []string          `json:"tags"`
	Timestamp time.Time         `json:"timestamp"`
}

func NewEventHandler(submitter EventSubmitter, logger *log.Logger) (http.Handler, error) {
	if submitter == nil {
		return nil, fmt.Errorf("%w: event submitter is required", session.ErrInvalidInput)
	}
	if logger == nil {
		return nil, fmt.Errorf("%w: logger is required", session.ErrInvalidInput)
	}
	h := &eventHandler{submitter: submitter, logger: logger, now: time.Now}
	mux := http.NewServeMux()
	mux.HandleFunc("POST /v1/events", h.serveEvent)
	return mux, nil
}

func (h *eventHandler) serveEvent(w http.ResponseWriter, r *http.Request) {
	payload, decoded, err := decodeEventRequest(r)
	if err != nil {
		h.deadLetter(payload, err, nil)
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	event, err := service.NewEvent(service.EventInput{
		EventID: decoded.EventID, Type: decoded.Type, TenantID: decoded.TenantID, Username: decoded.Username,
		IP: decoded.IP, Tags: decoded.Tags, Timestamp: decoded.Timestamp,
	})
	if err != nil {
		h.deadLetter(payload, err, nil)
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	if err := h.submitter.Submit(r.Context(), event); err != nil {
		status := eventStatus(err)
		if status == http.StatusBadRequest || status == http.StatusConflict {
			h.deadLetter(payload, err, &event)
		}
		writeError(w, status, err.Error())
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func decodeEventRequest(r *http.Request) ([]byte, eventRequest, error) {
	payload, err := io.ReadAll(io.LimitReader(r.Body, maximumEventBytes+1))
	if err != nil {
		return payload, eventRequest{}, fmt.Errorf("%w: read event envelope: %v", session.ErrInvalidInput, err)
	}
	if len(payload) > maximumEventBytes {
		return payload[:maximumEventBytes], eventRequest{}, fmt.Errorf("%w: event envelope exceeds %d bytes", session.ErrInvalidInput, maximumEventBytes)
	}
	decoder := json.NewDecoder(bytes.NewReader(payload))
	decoder.DisallowUnknownFields()
	var decoded eventRequest
	if err := decoder.Decode(&decoded); err != nil {
		return payload, eventRequest{}, fmt.Errorf("%w: malformed event envelope: %v", session.ErrInvalidInput, err)
	}
	if err := decoder.Decode(&struct{}{}); err != io.EOF {
		return payload, eventRequest{}, fmt.Errorf("%w: event envelope contains trailing data", session.ErrInvalidInput)
	}
	return payload, decoded, nil
}

func eventStatus(err error) int {
	switch {
	case errors.Is(err, context.Canceled), errors.Is(err, context.DeadlineExceeded):
		return http.StatusRequestTimeout
	case errors.Is(err, session.ErrInvalidTransition), errors.Is(err, session.ErrStaleEvent):
		return http.StatusConflict
	case errors.Is(err, session.ErrInvalidInput):
		return http.StatusBadRequest
	case errors.Is(err, eventstream.ErrClosed):
		return http.StatusServiceUnavailable
	default:
		return http.StatusServiceUnavailable
	}
}

func (h *eventHandler) deadLetter(payload []byte, cause error, event *service.Event) {
	entry := map[string]any{"kind": "dead_letter", "payload": string(payload), "reason": cause.Error(), "failedAt": h.now().UTC()}
	if event != nil {
		entry["sessionKey"] = event.Key
		if partitioner, ok := h.submitter.(interface{ PartitionFor(service.Event) int }); ok {
			entry["partitionId"] = partitioner.PartitionFor(*event)
		}
	}
	encoded, err := json.Marshal(entry)
	if err != nil {
		h.logger.Printf("dead letter encoding failed: %v", err)
		return
	}
	h.logger.Print(string(encoded))
}
