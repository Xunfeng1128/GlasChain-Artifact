// Package observability contains read-only diagnostics. It must not influence
// transaction admission, consensus, placement, or experiment completion.
package observability

import (
	"bufio"
	"bytes"
	"encoding/json"
	"fmt"
	"log"
	"math"
	"os"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"time"
)

// OSMemory distinguishes unavailable measurements from a measured zero. Linux
// exposes these values in procfs; other platforms still have Go heap samples.
// RuntimeSys is intentionally not used as a substitute for resident memory.
type OSMemory struct {
	Platform         string  `json:"platform"`
	ProcessAvailable bool    `json:"process_available"`
	SystemAvailable  bool    `json:"system_available"`
	ProcessRSSBytes  *uint64 `json:"process_rss_bytes,omitempty"`
	ProcessSwapBytes *uint64 `json:"process_swap_bytes,omitempty"`
	SystemTotalBytes *uint64 `json:"system_total_bytes,omitempty"`
	SystemAvailBytes *uint64 `json:"system_available_bytes,omitempty"`
	SystemSwapTotal  *uint64 `json:"system_swap_total_bytes,omitempty"`
	SystemSwapFree   *uint64 `json:"system_swap_free_bytes,omitempty"`
	ProcessError     string  `json:"process_error,omitempty"`
	SystemError      string  `json:"system_error,omitempty"`
}

type MemorySample struct {
	SchemaVersion  int      `json:"schema_version"`
	PID            int      `json:"pid"`
	UnixNS         int64    `json:"unix_ns"`
	Stage          string   `json:"stage"`
	Epoch          uint64   `json:"epoch"`
	HeapAlloc      uint64   `json:"heap_alloc"`
	HeapInuse      uint64   `json:"heap_inuse"`
	HeapIdle       uint64   `json:"heap_idle"`
	HeapReleased   uint64   `json:"heap_released"`
	RuntimeSys     uint64   `json:"runtime_sys"`
	TotalAlloc     uint64   `json:"total_alloc"`
	NumGC          uint32   `json:"num_gc"`
	GCPauseTotalNS uint64   `json:"gc_pause_total_ns"`
	Goroutines     int      `json:"goroutines"`
	OS             OSMemory `json:"os"`
}

// LogMemory samples the current supervisor process without forcing GC. Sampling
// and log output have a small observation cost; all errors remain diagnostic.
func LogMemory(logger *log.Logger, stage string, epoch uint64) {
	if logger == nil {
		return
	}
	var memory runtime.MemStats
	runtime.ReadMemStats(&memory)
	sample := MemorySample{
		SchemaVersion: 1, PID: os.Getpid(), UnixNS: time.Now().UnixNano(),
		Stage: stage, Epoch: epoch, HeapAlloc: memory.HeapAlloc,
		HeapInuse: memory.HeapInuse, HeapIdle: memory.HeapIdle,
		HeapReleased: memory.HeapReleased, RuntimeSys: memory.Sys,
		TotalAlloc: memory.TotalAlloc, NumGC: memory.NumGC,
		GCPauseTotalNS: memory.PauseTotalNs, Goroutines: runtime.NumGoroutine(),
		OS: readOSMemory(runtime.GOOS, os.ReadFile),
	}
	payload, err := json.Marshal(sample)
	if err != nil {
		logger.Printf("SUPERVISOR_MEMORY_ERROR stage=%s epoch=%d error=%v", stage, epoch, err)
		return
	}
	logger.Printf("SUPERVISOR_MEMORY %s", payload)
}

// StartMemoryObserver emits startup, periodic, and shutdown samples. The stop
// function is idempotent and waits for the observer to exit. No control-flow or
// transaction locks are acquired; callers own the observer's lifetime.
func StartMemoryObserver(logger *log.Logger, interval time.Duration) func() {
	if logger == nil {
		return func() {}
	}
	return startMemoryObserver(func(stage string) { LogMemory(logger, stage, 0) }, interval)
}

func startMemoryObserver(sample func(string), interval time.Duration) func() {
	if interval <= 0 {
		interval = 30 * time.Second
	}
	sample("startup")
	stop, done := make(chan struct{}), make(chan struct{})
	go func() {
		defer close(done)
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		for {
			select {
			case <-stop:
				sample("shutdown")
				return
			case <-ticker.C:
				sample("periodic")
			}
		}
	}()
	var once sync.Once
	return func() {
		once.Do(func() { close(stop) })
		<-done
	}
}

func readOSMemory(platform string, readFile func(string) ([]byte, error)) OSMemory {
	result := OSMemory{Platform: platform}
	if platform != "linux" {
		result.ProcessError, result.SystemError = "unsupported_platform", "unsupported_platform"
		return result
	}
	read := func(path string, keys ...string) (map[string]uint64, error) {
		data, err := readFile(path)
		if err != nil {
			return nil, err
		}
		return parseProcKB(data, keys...)
	}
	if values, err := read("/proc/self/status", "VmRSS", "VmSwap"); err != nil {
		result.ProcessError = err.Error()
	} else {
		rss, swap := values["VmRSS"], values["VmSwap"]
		result.ProcessAvailable = true
		result.ProcessRSSBytes, result.ProcessSwapBytes = &rss, &swap
	}
	if values, err := read("/proc/meminfo", "MemTotal", "MemAvailable", "SwapTotal", "SwapFree"); err != nil {
		result.SystemError = err.Error()
	} else {
		total, available := values["MemTotal"], values["MemAvailable"]
		swapTotal, swapFree := values["SwapTotal"], values["SwapFree"]
		result.SystemAvailable = true
		result.SystemTotalBytes, result.SystemAvailBytes = &total, &available
		result.SystemSwapTotal, result.SystemSwapFree = &swapTotal, &swapFree
	}
	return result
}

// Linux procfs labels these values kB, meaning 1024 bytes. A missing or malformed
// required field makes that measurement unavailable rather than reporting zero.
func parseProcKB(data []byte, keys ...string) (map[string]uint64, error) {
	wanted := make(map[string]bool, len(keys))
	for _, key := range keys {
		wanted[key] = true
	}
	values := make(map[string]uint64, len(keys))
	scanner := bufio.NewScanner(bytes.NewReader(data))
	for scanner.Scan() {
		parts := strings.SplitN(scanner.Text(), ":", 2)
		if len(parts) != 2 || !wanted[parts[0]] {
			continue
		}
		key := parts[0]
		if _, exists := values[key]; exists {
			return nil, fmt.Errorf("duplicate procfs field %s", key)
		}
		fields := strings.Fields(parts[1])
		if len(fields) != 2 || fields[1] != "kB" {
			return nil, fmt.Errorf("invalid procfs unit for %s", key)
		}
		value, err := strconv.ParseUint(fields[0], 10, 64)
		if err != nil || value > math.MaxUint64/1024 {
			return nil, fmt.Errorf("invalid procfs value for %s", key)
		}
		values[key] = value * 1024
	}
	if err := scanner.Err(); err != nil {
		return nil, err
	}
	for _, key := range keys {
		if _, exists := values[key]; !exists {
			return nil, fmt.Errorf("missing procfs field %s", key)
		}
	}
	return values, nil
}
