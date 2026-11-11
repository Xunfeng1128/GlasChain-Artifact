package networks

import (
	"blockEmulator/params"
	"bytes"
	"context"
	"io"
	"log"
	"net"
	"sync"
	"time"

	"math/rand"

	"golang.org/x/time/rate"
)

// pooledConn 在连接池中封装一条 TCP 连接和它专属的写锁。
// 让不同连接之间的写互不阻塞，同一连接上的并发写做串行化（避免字节交错）。
type pooledConn struct {
	conn      net.Conn
	writeGate chan struct{}
}

var connMaplock sync.Mutex
var connectionPool = make(map[string]*pooledConn, 0)
var poolContext, cancelPool = context.WithCancel(context.Background())

// Backpressure is applied before starting async send goroutines. Large control
// messages also have caller-side single-flight guards; this is the final bound.
var asyncSendSlots = make(chan struct{}, 128)

// network params.
var randomDelayGenerator *rand.Rand
var randomDelayLock sync.Mutex
var rateLimiterDownload *rate.Limiter
var rateLimiterUpload *rate.Limiter

func simulatedNetworkDelay() int {
	thisDelay := params.Delay
	if params.JitterRange == 0 {
		return thisDelay
	}
	randomDelayLock.Lock()
	if randomDelayGenerator == nil {
		randomDelayGenerator = rand.New(rand.NewSource(time.Now().UnixNano()))
	}
	thisDelay = randomDelayGenerator.Intn(params.JitterRange) - params.JitterRange/2 + params.Delay
	randomDelayLock.Unlock()
	if thisDelay < 0 {
		return 0
	}
	return thisDelay
}

// getPooledConn 从连接池中取连接；若不存在或已失效则建立新连接。
// 只在 map 查找/插入时短暂持有 connMaplock，写数据时完全不持有。
func getPooledConn(ctx context.Context, addr string) (*pooledConn, error) {
	connMaplock.Lock()
	pc := connectionPool[addr]
	connMaplock.Unlock()

	if pc != nil {
		if tcpConn, tcpOk := pc.conn.(*net.TCPConn); tcpOk {
			if err := tcpConn.SetKeepAlive(true); err != nil {
				removePooledConn(addr, pc)
				return dialAndStoreConn(ctx, addr)
			}
		}
		return pc, nil
	}

	return dialAndStoreConn(ctx, addr)
}

func dialAndStoreConn(ctx context.Context, addr string) (*pooledConn, error) {
	conn, err := (&net.Dialer{Timeout: 10 * time.Second}).DialContext(ctx, "tcp", addr)
	if err != nil {
		log.Println("Connect error", err)
		return nil, err
	}

	newPC := &pooledConn{conn: conn, writeGate: make(chan struct{}, 1)}

	connMaplock.Lock()
	if ctx.Err() != nil || poolContext.Err() != nil {
		connMaplock.Unlock()
		conn.Close()
		return nil, context.Canceled
	}
	// 并发 dial 的竞态：可能已被别的 goroutine 写入。优先复用既存连接。
	if existing := connectionPool[addr]; existing != nil {
		connMaplock.Unlock()
		conn.Close()
		return existing, nil
	}
	connectionPool[addr] = newPC
	connMaplock.Unlock()

	return newPC, nil
}

func removePooledConn(addr string, pc *pooledConn) {
	connMaplock.Lock()
	if connectionPool[addr] == pc {
		delete(connectionPool, addr)
	}
	connMaplock.Unlock()
	pc.conn.Close()
}

// Define the latency, jitter and bandwidth here.
// Init tools.
func InitNetworkTools() {
	connMaplock.Lock()
	if poolContext.Err() != nil {
		poolContext, cancelPool = context.WithCancel(context.Background())
	}
	connMaplock.Unlock()
	// avoid wrong params.
	if params.Delay < 0 {
		params.Delay = 0
	}
	if params.JitterRange < 0 {
		params.JitterRange = 0
	}
	if params.Bandwidth < 0 {
		params.Bandwidth = 0x7fffffff
	}

	// generate the random seed.
	randomDelayLock.Lock()
	randomDelayGenerator = rand.New(rand.NewSource(time.Now().UnixMicro()))
	randomDelayLock.Unlock()
	// Limit the download rate
	rateLimiterDownload = rate.NewLimiter(rate.Limit(params.Bandwidth), params.Bandwidth)
	// Limit the upload rate
	rateLimiterUpload = rate.NewLimiter(rate.Limit(params.Bandwidth), params.Bandwidth)
}

func defaultSendTimeout(size int) time.Duration {
	budget := 30*time.Second + time.Duration(params.Delay+params.JitterRange)*time.Millisecond
	if params.Bandwidth > 0 {
		budget += time.Duration(float64(size) / float64(params.Bandwidth) * float64(2*time.Second))
	}
	return budget
}

// TcpDialContext writes a complete framed message synchronously. Cancellation
// interrupts delay, connection acquisition, writer wait, bandwidth wait and IO.
// The payload must remain immutable until this function returns.
func TcpDialContext(ctx context.Context, payload []byte, addr string) error {
	_, _, err := sendContext(ctx, payload, addr)
	return err
}

func sendContext(parent context.Context, payload []byte, addr string) (time.Duration, time.Duration, error) {
	started := time.Now()
	ctx, cancel := context.WithTimeout(parent, defaultSendTimeout(len(payload)))
	defer cancel()
	connMaplock.Lock()
	poolDone := poolContext.Done()
	connMaplock.Unlock()
	watchDone := make(chan struct{})
	go func() {
		select {
		case <-poolDone:
			cancel()
		case <-watchDone:
		}
	}()
	defer close(watchDone)
	timer := time.NewTimer(time.Duration(simulatedNetworkDelay()) * time.Millisecond)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return time.Since(started), 0, ctx.Err()
	case <-timer.C:
	}
	pc, err := getPooledConn(ctx, addr)
	if err != nil {
		return time.Since(started), 0, err
	}
	select {
	case <-ctx.Done():
		return time.Since(started), 0, ctx.Err()
	case pc.writeGate <- struct{}{}:
	}
	defer func() { <-pc.writeGate }()
	writeStart := time.Now()
	deadline, _ := ctx.Deadline()
	if err := pc.conn.SetWriteDeadline(deadline); err != nil {
		removePooledConn(addr, pc)
		return writeStart.Sub(started), 0, err
	}
	// Closing a socket is safe concurrently with Write and releases a blocked
	// syscall. Never require the writer gate in this cancellation path.
	writeDone, watcherDone := make(chan struct{}), make(chan struct{})
	go func() {
		defer close(watcherDone)
		select {
		case <-ctx.Done():
			pc.conn.Close()
		case <-writeDone:
		}
	}()
	err = writeMessageToConnContext(ctx, payload, pc.conn, rateLimiterUpload)
	close(writeDone)
	<-watcherDone
	if err != nil || ctx.Err() != nil {
		removePooledConn(addr, pc)
		if ctx.Err() != nil {
			err = ctx.Err()
		} else if !time.Now().Before(deadline) {
			err = context.DeadlineExceeded
		}
	}
	return writeStart.Sub(started), time.Since(writeStart), err
}

func tcpDial(payload []byte, addr string, done func(error, time.Duration, time.Duration, time.Duration)) {
	start := time.Now()
	ctx, cancel := context.WithTimeout(context.Background(), defaultSendTimeout(len(payload)))
	connMaplock.Lock()
	poolDone := poolContext.Done()
	connMaplock.Unlock()
	select {
	case asyncSendSlots <- struct{}{}:
	case <-ctx.Done():
		cancel()
		if done != nil {
			done(ctx.Err(), time.Since(start), 0, time.Since(start))
		}
		return
	case <-poolDone:
		cancel()
		if done != nil {
			done(context.Canceled, time.Since(start), 0, time.Since(start))
		}
		return
	}
	go func() {
		defer func() { <-asyncSendSlots }()
		defer cancel()
		wait, write, err := sendContext(ctx, payload, addr)
		if err != nil {
			log.Printf("network send failed addr=%s bytes=%d: %v", addr, len(payload), err)
		}
		if done != nil {
			done(err, wait, write, time.Since(start))
		}
	}()
}

func TcpDial(payload []byte, addr string) {
	tcpDial(payload, addr, nil)
}

// TcpDialWithDone is the completion-aware variant used by large control-plane
// deliveries. done is invoked exactly once after the write finishes or the
// connection attempt fails, allowing callers to enforce one in-flight send per
// destination instead of building an unbounded goroutine/write-lock backlog.
func TcpDialWithDone(payload []byte, addr string, done func()) {
	tcpDial(payload, addr, func(_ error, _, _, _ time.Duration) {
		if done != nil {
			done()
		}
	})
}

func TcpDialWithTrace(payload []byte, addr string, traceName string, done func(error, time.Duration, time.Duration, time.Duration)) {
	tcpDial(payload, addr, func(err error, wait, write, total time.Duration) {
		log.Printf("DIAG NET send done: trace=%s addr=%s bytes=%d waitBeforeWrite=%s writeCost=%s totalCost=%s err=%v", traceName, addr, len(payload), wait, write, total, err)
		if done != nil {
			done(err, wait, write, total)
		}
	})
}

// Broadcast sends a message to multiple receivers, excluding the sender.
func Broadcast(sender string, receivers []string, msg []byte) {
	for _, ip := range receivers {
		if ip == sender {
			continue
		}
		TcpDial(msg, ip)
	}
}

// CloseAllConnInPool closes all connections in the connection pool.
func CloseAllConnInPool() {
	connMaplock.Lock()
	cancelPool()
	conns := connectionPool
	connectionPool = make(map[string]*pooledConn) // Reset the pool
	connMaplock.Unlock()

	for _, pc := range conns {
		pc.conn.Close()
	}
}

// ReadFromConn reads data from a connection.
func ReadFromConn(addr string) {
	connMaplock.Lock()
	pc := connectionPool[addr]
	connMaplock.Unlock()
	if pc == nil {
		log.Println("Read error for address", addr, ": connection is not built")
		return
	}
	conn := pc.conn

	// new a conn reader
	connReader := NewConnReader(conn, rateLimiterDownload)

	buffer := make([]byte, 1024)
	var messageBuffer bytes.Buffer

	for {
		n, err := connReader.Read(buffer)
		if err != nil {
			if err != io.EOF {
				log.Println("Read error for address", addr, ":", err)
			}
			break
		}

		// add message to buffer
		messageBuffer.Write(buffer[:n])

		// handle the full message
		for {
			message, err := readMessage(&messageBuffer)
			if err == io.ErrShortBuffer {
				// continue to load if buffer is short
				break
			} else if err == nil {
				// log the full message
				log.Println("Received from", addr, ":", message)
			} else {
				// handle other errs
				log.Println("Error processing message for address", addr, ":", err)
				break
			}
		}
	}
}

func readMessage(buffer *bytes.Buffer) (string, error) {
	message, err := buffer.ReadBytes('\n')
	if err != nil && err != io.EOF {
		return "", err
	}
	return string(message), nil
}
