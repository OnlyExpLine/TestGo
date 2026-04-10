package notifier

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

type MockClient struct {
	mu        sync.Mutex
	callCount int
	responses []int
	idx       int
	delay     time.Duration
}

func NewMockClient(responses ...int) *MockClient {
	return &MockClient{
		responses: responses,
	}
}

func (m *MockClient) Post(ctx context.Context, msg Message) (int, error) {
	m.mu.Lock()
	m.callCount++
	m.mu.Unlock()

	if m.delay > 0 {
		select {
		case <-time.After(m.delay):
		case <-ctx.Done():
			return 0, ctx.Err()
		}
	}
	m.mu.Lock()
	defer m.mu.Unlock()

	if len(m.responses) == 0 {
		return 200, nil
	}

	if m.idx >= len(m.responses) {
		return m.responses[len(m.responses)-1], nil
	}

	code := m.responses[m.idx]
	m.idx++
	return code, nil
}

func (m *MockClient) CallCount() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.callCount
}

// Тест 1: Успешная отправка
func TestSendSuccess(t *testing.T) {
	client := NewMockClient(200)
	n := New(client, 2, 100)
	defer n.Close()

	err := n.Send(context.Background(), Message{ID: "1", Payload: "test"})
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}

	stats := n.Stats()
	if stats.Sent != 1 {
		t.Errorf("expected 1 sent, got %d", stats.Sent)
	}
	if stats.Failed != 0 {
		t.Errorf("expected 0 failed, got %d", stats.Failed)
	}
}

// Тест 2: Retry при 429
func TestRetryOn429(t *testing.T) {
	client := NewMockClient(429, 429, 200)
	n := New(client, 1, 100)
	defer n.Close()

	err := n.Send(context.Background(), Message{ID: "1"})
	if err != nil {
		t.Fatalf("expected success, got %v", err)
	}

	stats := n.Stats()
	if stats.Sent != 1 {
		t.Errorf("expected 1 sent, got %d", stats.Sent)
	}
	if stats.Retries != 2 {
		t.Errorf("expected 2 retries, got %d", stats.Retries)
	}
	if stats.Failed != 0 {
		t.Errorf("expected 0 failed, got %d", stats.Failed)
	}
}

// Тест 3: Превышение попыток
func TestMaxRetries(t *testing.T) {
	client := NewMockClient(429, 429, 429, 429)
	n := New(client, 1, 100)
	defer n.Close()

	err := n.Send(context.Background(), Message{ID: "1"})
	if err == nil {
		t.Fatal("expected error, got nil")
	}

	stats := n.Stats()
	if stats.Sent != 0 {
		t.Errorf("expected 0 sent, got %d", stats.Sent)
	}
	if stats.Retries != 2 {
		t.Errorf("expected 2 retries, got %d", stats.Retries)
	}
	if stats.Failed != 1 {
		t.Errorf("expected 1 failed, got %d", stats.Failed)
	}
}

// Тест 4: Не 429 ошибка
func TestNonRetryableError(t *testing.T) {
	client := NewMockClient(500)
	n := New(client, 1, 100)
	defer n.Close()

	err := n.Send(context.Background(), Message{ID: "1"})
	if err == nil {
		t.Fatal("expected error, got nil")
	}

	stats := n.Stats()
	if stats.Sent != 0 {
		t.Errorf("expected 0 sent, got %d", stats.Sent)
	}
	if stats.Retries != 0 {
		t.Errorf("expected 0 retries, got %d", stats.Retries)
	}
	if stats.Failed != 1 {
		t.Errorf("expected 1 failed, got %d", stats.Failed)
	}

	if client.CallCount() != 1 {
		t.Errorf("expected 1 call, got %d", client.CallCount())
	}
}

// Тест 5: Rate limiting
func TestRateLimit(t *testing.T) {
	client := NewMockClient(200, 200, 200, 200, 200, 200)
	n := New(client, 1, 2)
	defer n.Close()

	start := time.Now()

	for i := 0; i < 4; i++ {
		err := n.Send(context.Background(), Message{ID: fmt.Sprintf("%d", i)})
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
	}

	elapsed := time.Since(start)

	if elapsed < 1500*time.Millisecond {
		t.Errorf("rate limiter not working, elapsed: %v", elapsed)
	}
}

// Тест 6: Отмена контекста
func TestContextCancel(t *testing.T) {
	client := NewMockClient(200)
	client.delay = 500 * time.Millisecond
	n := New(client, 1, 100)
	defer n.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()

	err := n.Send(ctx, Message{ID: "1"})
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Errorf("expected DeadlineExceeded, got %v", err)
	}
}

// Тест 7: Закрытие Notifier
func TestClose(t *testing.T) {
	client := NewMockClient(200)
	n := New(client, 2, 100)

	var wg sync.WaitGroup
	for i := 0; i < 5; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			_ = n.Send(context.Background(), Message{ID: fmt.Sprintf("%d", id)})
		}(i)
	}

	time.Sleep(50 * time.Millisecond)

	err := n.Close()
	if err != nil {
		t.Fatalf("close error: %v", err)
	}
	wg.Wait()

	err = n.Send(context.Background(), Message{ID: "after_close"})
	if !errors.Is(err, ErrClosed) {
		t.Errorf("expected ErrClosed, got %v", err)
	}
}

// Тест 8: Проверка на гонку потоков
func TestConcurrentSend(t *testing.T) {
	client := NewMockClient(200)
	n := New(client, 10, 1000)
	defer n.Close()

	var wg sync.WaitGroup
	var successCount atomic.Int32

	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			err := n.Send(context.Background(), Message{ID: fmt.Sprintf("%d", id)})
			if err == nil {
				successCount.Add(1)
			}
		}(i)
	}

	wg.Wait()

	stats := n.Stats()
	if int(stats.Sent) != 100 {
		t.Errorf("expected 100 sent, got %d", stats.Sent)
	}
	if int(successCount.Load()) != 100 {
		t.Errorf("expected 100 successes, got %d", successCount.Load())
	}
}

// Тест 9: Статистика после множества операций
func TestStatsAccuracy(t *testing.T) {
	client := NewMockClient(200, 429, 429, 429, 200)
	n := New(client, 2, 100)
	defer n.Close()

	n.Send(context.Background(), Message{ID: "1"})
	n.Send(context.Background(), Message{ID: "2"})
	n.Send(context.Background(), Message{ID: "3"})

	time.Sleep(100 * time.Millisecond)

	stats := n.Stats()
	if stats.Sent != 2 {
		t.Errorf("expected 2 sent, got %d", stats.Sent)
	}
	if stats.Failed != 1 {
		t.Errorf("expected 1 failed, got %d", stats.Failed)
	}

	if stats.Retries != 2 {
		t.Errorf("expected 2 retries, got %d", stats.Retries)
	}
}

// Тест 10: Graceful shutdown дожидается обработки
func TestGracefulShutdown(t *testing.T) {
	client := NewMockClient(200)
	client.delay = 100 * time.Millisecond
	n := New(client, 2, 100)

	for i := 0; i < 5; i++ {
		n.Send(context.Background(), Message{ID: fmt.Sprintf("%d", i)})
	}

	n.Close()

	stats := n.Stats()
	if stats.Sent != 5 {
		t.Errorf("expected 5 sent after close, got %d", stats.Sent)
	}
}
