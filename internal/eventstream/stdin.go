package eventstream

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"strings"
	"sync"
	"time"

	"github.com/Bennybl/session-handler/internal/session"
)

const maximumEventBytes = 1024 * 1024

type StdinSource struct {
	mu      sync.RWMutex
	scanMu  sync.Mutex
	scanner *bufio.Scanner
	closer  io.Closer
	closed  bool
}

func NewStdinSource(reader io.Reader) (*StdinSource, error) {
	if reader == nil {
		return nil, fmt.Errorf("%w: stdin reader is required", session.ErrInvalidInput)
	}
	scanner := bufio.NewScanner(reader)
	scanner.Buffer(make([]byte, 64*1024), maximumEventBytes)
	source := &StdinSource{scanner: scanner}
	if closer, ok := reader.(io.Closer); ok {
		source.closer = closer
	}
	return source, nil
}

func (s *StdinSource) Next(ctx context.Context) (Message, error) {
	if ctx == nil {
		return nil, fmt.Errorf("%w: context is required", session.ErrInvalidInput)
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	s.scanMu.Lock()
	defer s.scanMu.Unlock()
	s.mu.RLock()
	closed := s.closed
	s.mu.RUnlock()
	if closed {
		return nil, io.EOF
	}
	for {
		scanned := make(chan bool, 1)
		go func() { scanned <- s.scanner.Scan() }()
		var ok bool
		select {
		case <-ctx.Done():
			s.closeReader()
			return nil, ctx.Err()
		case ok = <-scanned:
		}
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		if !ok {
			if err := s.scanner.Err(); err != nil {
				return nil, fmt.Errorf("read NDJSON event: %w", err)
			}
			return nil, io.EOF
		}
		line := append([]byte(nil), s.scanner.Bytes()...)
		if strings.TrimSpace(string(line)) == "" {
			continue
		}
		return stdinMessage{data: line}, nil
	}
}

func (s *StdinSource) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return nil
	}
	s.closed = true
	if s.closer != nil {
		return s.closer.Close()
	}
	return nil
}

func (s *StdinSource) closeReader() {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return
	}
	s.closed = true
	if s.closer != nil {
		_ = s.closer.Close()
	}
}

type stdinMessage struct {
	data []byte
}

func (m stdinMessage) Data() []byte { return m.data }

func (stdinMessage) Ack(context.Context) error { return nil }

func (stdinMessage) NakWithDelay(time.Duration) error {
	return fmt.Errorf("stdin events cannot be redelivered")
}

func (stdinMessage) Term() error { return nil }
