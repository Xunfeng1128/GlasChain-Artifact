package networks

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net"
	"os"
	"syscall"
	"testing"
	"time"

	"golang.org/x/time/rate"
)

// The framing code needs only Write. Wrap ENOBUFS as a real TCP write does.
type bufferFaultConn struct {
	net.Conn
	write func([]byte) (int, error)
}

func (c bufferFaultConn) Write(p []byte) (int, error) { return c.write(p) }

func socketBufferFull() error {
	return &net.OpError{Op: "write", Net: "tcp", Err: os.NewSyscallError("write", syscall.ENOBUFS)}
}

func TestFrameRecoversPartialBufferPressureWithoutRechargingOrDuplicating(t *testing.T) {
	payload := []byte(`{"stage":"PrePrepare","sequence":22}`)
	var output bytes.Buffer
	attempts, delimiterAttempts := 0, 0
	conn := bufferFaultConn{write: func(p []byte) (int, error) {
		if bytes.Equal(p, []byte{'\n'}) {
			delimiterAttempts++
			if delimiterAttempts == 1 {
				return 0, socketBufferFull()
			}
			return output.Write(p)
		}
		attempts++
		switch attempts {
		case 1:
			n, _ := output.Write(p[:7])
			return n, socketBufferFull()
		case 2:
			return 0, socketBufferFull()
		default:
			return output.Write(p)
		}
	}}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	// One burst pays for exactly this payload and its delimiter. Recharging for
	// bytes already reserved would take 1,000 s per byte and miss the deadline.
	if err := writeMessageToConnContext(ctx, payload, conn, rate.NewLimiter(0.001, len(payload)+1)); err != nil {
		t.Fatal(err)
	}
	if output.String() != string(payload)+"\n" || delimiterAttempts != 2 {
		t.Fatalf("received %q after %d delimiter attempts", output.Bytes(), delimiterAttempts)
	}
}

func TestFrameAdaptsOnlyAfterLargeWriteBufferPressure(t *testing.T) {
	payload := bytes.Repeat([]byte{'x'}, 1<<20)
	for _, pressure := range []bool{false, true} {
		t.Run(map[bool]string{false: "healthy", true: "pressure"}[pressure], func(t *testing.T) {
			var output bytes.Buffer
			var writes []int
			conn := bufferFaultConn{write: func(p []byte) (int, error) {
				writes = append(writes, len(p))
				if pressure && len(p) == len(payload) {
					return 0, socketBufferFull()
				}
				return output.Write(p)
			}}
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			if err := writeMessageToConnContext(ctx, payload, conn, rate.NewLimiter(rate.Inf, len(payload))); err != nil {
				t.Fatal(err)
			}
			if writes[0] != len(payload) || (!pressure && len(writes) != 2) {
				t.Fatalf("unexpected write sizes: %v", writes)
			}
			if output.Len() != len(payload)+1 || !bytes.Equal(output.Bytes()[:len(payload)], payload) || output.Bytes()[len(payload)] != '\n' {
				t.Fatal("wire frame was incomplete or duplicated")
			}
		})
	}
}

func TestRateLimitedWriterStopsAfterPartialPermanentWrite(t *testing.T) {
	calls := 0
	w := rateLimitedWriter{ctx: context.Background(), limiter: rate.NewLimiter(rate.Inf, 128), writer: bufferFaultConn{write: func([]byte) (int, error) {
		calls++
		return 3, io.ErrClosedPipe
	}}}
	n, err := w.Write([]byte("partially sent"))
	if n != 3 || !errors.Is(err, io.ErrClosedPipe) || calls != 1 {
		t.Fatalf("wrote %d, err=%v, attempts=%d", n, err, calls)
	}
}

func TestRateLimitedWriterPersistentBufferPressureHonorsDeadline(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	calls := 0
	w := rateLimitedWriter{ctx: ctx, limiter: rate.NewLimiter(rate.Inf, 128), writer: bufferFaultConn{write: func([]byte) (int, error) {
		calls++
		if calls == 1 {
			return 3, socketBufferFull()
		}
		return 0, socketBufferFull()
	}}}
	n, err := w.Write([]byte("partially sent"))
	if n != 3 || !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("wrote %d, err=%v, attempts=%d", n, err, calls)
	}
}
