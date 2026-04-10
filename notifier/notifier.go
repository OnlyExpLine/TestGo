package notifier

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"time"

	"golang.org/x/time/rate"
)

type Message struct {
	ID      string
	Payload string
}

type ExternalClient interface {
	Post(ctx context.Context, msg Message) (statusCode int, err error)
}

type Notifier interface {
	Send(ctx context.Context, msg Message) error
	Close() error
	Stats() Stats
}

type task struct {
	msg     Message
	ctx     context.Context
	errChan chan error
}

type notifierImpl struct {
	client  ExternalClient
	limiter *rate.Limiter

	jobChan chan *task
	done    chan struct{}

	wg sync.WaitGroup

	sent    atomic.Int64
	failed  atomic.Int64
	retries atomic.Int64
}

var ErrClosed = errors.New("notifier is closed")

func New(client ExternalClient, workers, rateLimit int) Notifier {
	if workers < 1 {
		workers = 1
	}
	if rateLimit < 1 {
		rateLimit = 1
	}
	n := &notifierImpl{
		client:  client,
		limiter: rate.NewLimiter(rate.Every(time.Second/time.Duration(rateLimit)), 1),
		jobChan: make(chan *task, 1000),
		done:    make(chan struct{}),
	}

	for i := 0; i < workers; i++ {
		n.wg.Add(1)
		go n.worker()
	}

	return n
}

func (n *notifierImpl) Send(ctx context.Context, msg Message) error {
	t := &task{
		msg:     msg,
		ctx:     ctx,
		errChan: make(chan error, 1),
	}

	select {
	case n.jobChan <- t:
	case <-ctx.Done():
		return ctx.Err()
	case <-n.done:
		return ErrClosed
	}

	select {
	case err := <-t.errChan:
		return err
	case <-ctx.Done():
		return ctx.Err()
	case <-n.done:
		return ErrClosed
	}
}

func (n *notifierImpl) Close() error {
	select {
	case <-n.done:
		return nil
	default:
		close(n.done)
	}

	n.wg.Wait()
	return nil
}
func (n *notifierImpl) Stats() Stats {
	return Stats{
		Sent:    n.sent.Load(),
		Failed:  n.failed.Load(),
		Retries: n.retries.Load(),
	}
}

func (n *notifierImpl) worker() {
	defer n.wg.Done()

	for {
		select {
		case t := <-n.jobChan:
			if t == nil {
				continue
			}

			err := n.processWithRetry(t)

			select {
			case t.errChan <- err:
			default:
			}
			close(t.errChan)

		case <-n.done:
			return
		}
	}
}

func (n *notifierImpl) processWithRetry(t *task) error {
	const maxAttempts = 3

	for attempt := 1; attempt <= maxAttempts; attempt++ {
		if attempt > 1 {
			n.retries.Add(1)
		}

		if err := n.limiter.Wait(t.ctx); err != nil {
			n.failed.Add(1)
			return err
		}

		statusCode, err := n.client.Post(t.ctx, t.msg)

		if err == nil && statusCode >= 200 && statusCode < 300 {
			n.sent.Add(1)
			return nil
		}

		if statusCode != 429 || attempt == maxAttempts {
			n.failed.Add(1)
			if err != nil {
				return err
			}
			return errors.New("request failed")
		}

		backoff := time.Duration(100*(1<<(attempt-1))) * time.Millisecond

		select {
		case <-time.After(backoff):
		case <-t.ctx.Done():
			n.failed.Add(1)
			return t.ctx.Err()
		case <-n.done:
			n.failed.Add(1)
			return ErrClosed
		}
	}

	n.failed.Add(1)
	return errors.New("max retries exceeded")
}
