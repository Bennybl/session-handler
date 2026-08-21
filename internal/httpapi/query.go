package httpapi

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"time"

	"github.com/Bennybl/session-handler/internal/repository"
	"github.com/Bennybl/session-handler/internal/service"
	"github.com/Bennybl/session-handler/internal/session"
)

const maximumQueryBytes = 1024 * 1024

type QueryService interface {
	Query(context.Context, service.QueryRequest) (session.QueryResult, error)
}

type handler struct {
	query QueryService
}

type queryRequest struct {
	Filters []filterRequest `json:"filters"`
	Page    pageRequest     `json:"page"`
}

type filterRequest struct {
	Field    session.Field    `json:"field"`
	Operator session.Operator `json:"operator"`
	Value    any              `json:"value"`
}

type pageRequest struct {
	Limit  int    `json:"limit"`
	Cursor string `json:"cursor"`
}

type queryResponse struct {
	Sessions   []sessionResponse `json:"sessions"`
	NextCursor string            `json:"nextCursor,omitempty"`
}

type sessionResponse struct {
	ID       string          `json:"id"`
	TenantID string          `json:"tenantId"`
	Username string          `json:"username"`
	IP       string          `json:"ip"`
	LoginAt  time.Time       `json:"loginAt"`
	LogoutAt *time.Time      `json:"logoutAt"`
	States   []stateResponse `json:"states"`
}

type stateResponse struct {
	Tags      []string   `json:"tags"`
	ValidFrom time.Time  `json:"validFrom"`
	ValidTo   *time.Time `json:"validTo"`
}

type errorResponse struct {
	Error string `json:"error"`
}

func NewHandler(query QueryService) (http.Handler, error) {
	if query == nil {
		return nil, fmt.Errorf("%w: query service is required", session.ErrInvalidInput)
	}
	h := &handler{query: query}
	mux := http.NewServeMux()
	mux.HandleFunc("POST /v1/sessions/query", h.serveQuery)
	return mux, nil
}

func (h *handler) serveQuery(responseWriter http.ResponseWriter, request *http.Request) {
	var input queryRequest
	if err := decodeQueryRequest(responseWriter, request, &input); err != nil {
		writeError(responseWriter, http.StatusBadRequest, err.Error())
		return
	}

	filters := make([]session.Filter, len(input.Filters))
	for index, filter := range input.Filters {
		filters[index] = session.Filter{Field: filter.Field, Operator: filter.Operator, Value: filter.Value}
	}
	result, err := h.query.Query(request.Context(), service.QueryRequest{
		Filters: filters,
		Page:    session.PageRequest{Limit: input.Page.Limit, Cursor: input.Page.Cursor},
	})
	if err != nil {
		if invalidQueryError(err) {
			writeError(responseWriter, http.StatusBadRequest, err.Error())
			return
		}
		writeError(responseWriter, http.StatusInternalServerError, "internal server error")
		return
	}

	writeJSON(responseWriter, http.StatusOK, toQueryResponse(result))
}

func decodeQueryRequest(responseWriter http.ResponseWriter, request *http.Request, destination *queryRequest) error {
	body := http.MaxBytesReader(responseWriter, request.Body, maximumQueryBytes)
	decoder := json.NewDecoder(body)
	decoder.DisallowUnknownFields()
	decoder.UseNumber()
	if err := decoder.Decode(destination); err != nil {
		return fmt.Errorf("invalid query request: %w", err)
	}
	if err := decoder.Decode(&struct{}{}); err != io.EOF {
		if err == nil {
			return fmt.Errorf("invalid query request: trailing JSON value")
		}
		return fmt.Errorf("invalid query request: %w", err)
	}
	return nil
}

func invalidQueryError(err error) bool {
	return errors.Is(err, repository.ErrInvalidQuery) ||
		errors.Is(err, repository.ErrInvalidCursor) ||
		errors.Is(err, session.ErrInvalidInput)
}

func toQueryResponse(result session.QueryResult) queryResponse {
	response := queryResponse{
		Sessions:   make([]sessionResponse, len(result.Sessions)),
		NextCursor: result.NextCursor,
	}
	for sessionIndex, value := range result.Sessions {
		states := make([]stateResponse, len(value.States))
		for stateIndex, state := range value.States {
			states[stateIndex] = stateResponse{
				Tags: append([]string{}, state.Tags...), ValidFrom: state.ValidFrom, ValidTo: state.ValidTo,
			}
		}
		response.Sessions[sessionIndex] = sessionResponse{
			ID: value.ID, TenantID: value.Key.TenantID, Username: value.Key.Username, IP: value.Key.IP,
			LoginAt: value.LoginAt, LogoutAt: value.LogoutAt, States: states,
		}
	}
	return response
}

func writeError(responseWriter http.ResponseWriter, status int, message string) {
	writeJSON(responseWriter, status, errorResponse{Error: message})
}

func writeJSON(responseWriter http.ResponseWriter, status int, value any) {
	var encoded bytes.Buffer
	if err := json.NewEncoder(&encoded).Encode(value); err != nil {
		http.Error(responseWriter, "internal server error", http.StatusInternalServerError)
		return
	}
	responseWriter.Header().Set("Content-Type", "application/json")
	responseWriter.WriteHeader(status)
	_, _ = responseWriter.Write(encoded.Bytes())
}
