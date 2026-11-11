package networks

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"syscall"
	"time"

	"golang.org/x/time/rate"
)

// Write to conn with bandwidth limit.
type rateLimitedWriter struct {
	writer  io.Writer
	limiter *rate.Limiter
	ctx     context.Context
}

// Write method waits for the rate limiter and then writes data
func (w *rateLimitedWriter) Write(p []byte) (int, error) {
	if w.limiter == nil || w.limiter.Burst() <= 0 {
		return 0, fmt.Errorf("invalid upload rate limiter")
	}
	// Calculate the number of bytes to write and wait for the limiter to grant enough tokens
	ctx := w.ctx
	if ctx == nil {
		ctx = context.Background()
	}
	written := 0
	for written < len(p) {
		writeByteNum := w.limiter.Burst()
		if writeByteNum > len(p)-written {
			writeByteNum = len(p) - written
		}
		if err := w.limiter.WaitN(ctx, writeByteNum); err != nil {
			return written, err
		}
		end := written + writeByteNum
		n, err := w.writer.Write(p[written:end])
		written += n
		if errors.Is(err, syscall.ENOBUFS) {
			// WaitN has already charged through end. Continue this same frame
			// on the locked connection, beyond any bytes the kernel accepted.
			n, err = writeAfterBufferPressure(ctx, w.writer, p[written:end])
			written += n
		}
		if err != nil {
			return written, err
		}
		if written != end {
			return written, io.ErrShortWrite
		}
	}
	return written, nil
}

// Large local TCP writes can return ENOBUFS during a burst of PBFT proposals.
// Smaller writes on the same stream preserve the original newline framing.
func writeAfterBufferPressure(ctx context.Context, dst io.Writer, remaining []byte) (int, error) {
	const writeSize = 64 << 10
	const maxDelay = 50 * time.Millisecond
	ctx, cancel := context.WithTimeout(ctx, defaultSendTimeout(len(remaining)))
	defer cancel()
	var written int
	delay := time.Millisecond
	for written < len(remaining) {
		if delay > 0 {
			select {
			case <-ctx.Done():
				return written, ctx.Err()
			case <-time.After(delay):
			}
		}
		if err := ctx.Err(); err != nil {
			return written, err
		}
		size := len(remaining) - written
		if size > writeSize {
			size = writeSize
		}
		n, err := dst.Write(remaining[written : written+size])
		written += n
		switch {
		case errors.Is(err, syscall.ENOBUFS):
			if n > 0 || delay == 0 {
				delay = time.Millisecond
			} else if delay < maxDelay {
				delay *= 2
				if delay > maxDelay {
					delay = maxDelay
				}
			}
		case err != nil:
			return written, err
		case n != size:
			return written, io.ErrShortWrite
		default:
			delay = 0
		}
	}
	return written, nil
}

func writeToConn(connMsg []byte, conn net.Conn, limiter *rate.Limiter) error {
	return writeToConnContext(context.Background(), connMsg, conn, limiter)
}

func writeToConnContext(ctx context.Context, connMsg []byte, conn net.Conn, limiter *rate.Limiter) error {
	// Wrap the connection with rateLimitedWriter
	rateLimitedConn := &rateLimitedWriter{writer: conn, limiter: limiter, ctx: ctx}

	// writing data to the connection
	written, err := rateLimitedConn.Write(connMsg)
	if err != nil {
		return err
	}
	if written != len(connMsg) {
		return io.ErrShortWrite
	}
	return nil
}

// writeMessageToConn writes the message and its delimiter while the caller
// holds the per-connection write lock. Keeping the delimiter as a second write
// avoids allocating and copying another multi-megabyte control payload merely
// to append one byte.
func writeMessageToConn(connMsg []byte, conn net.Conn, limiter *rate.Limiter) error {
	return writeMessageToConnContext(context.Background(), connMsg, conn, limiter)
}

func writeMessageToConnContext(ctx context.Context, connMsg []byte, conn net.Conn, limiter *rate.Limiter) error {
	if err := writeToConnContext(ctx, connMsg, conn, limiter); err != nil {
		return err
	}
	return writeToConnContext(ctx, []byte{'\n'}, conn, limiter)
}
