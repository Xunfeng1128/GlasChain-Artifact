package observability

import (
	"bytes"
	"encoding/json"
	"errors"
	"log"
	"strings"
	"sync"
	"testing"
	"time"
)

func TestLinuxMemoryDistinguishesMeasuredZeroFromUnavailable(t *testing.T) {
	files := map[string]string{
		"/proc/self/status": "Name:\tsupervisor\nVmRSS:\t12345 kB\nVmSwap:\t0 kB\nThreads:\t8\n",
		"/proc/meminfo":     "MemTotal: 32768000 kB\nMemAvailable: 4096000 kB\nSwapTotal: 2097152 kB\nSwapFree: 1048576 kB\n",
	}
	result := readOSMemory("linux", func(path string) ([]byte, error) { return []byte(files[path]), nil })
	if !result.ProcessAvailable || !result.SystemAvailable || result.ProcessError != "" || result.SystemError != "" {
		t.Fatalf("unexpected unavailable sample: %+v", result)
	}
	if *result.ProcessRSSBytes != 12345*1024 || result.ProcessSwapBytes == nil || *result.ProcessSwapBytes != 0 || *result.SystemAvailBytes != 4096000*1024 {
		t.Fatalf("incorrect bytes or measured zero: %+v", result)
	}
	encoded, err := json.Marshal(result)
	if err != nil || !strings.Contains(string(encoded), `"process_swap_bytes":0`) {
		t.Fatalf("measured zero disappeared: %s, %v", encoded, err)
	}
	unavailable := readOSMemory("linux", func(string) ([]byte, error) { return nil, errors.New("unavailable") })
	encoded, _ = json.Marshal(unavailable)
	if unavailable.ProcessAvailable || unavailable.SystemAvailable || strings.Contains(string(encoded), `"process_rss_bytes"`) || unavailable.ProcessError == "" {
		t.Fatalf("unavailable measurements look like valid zero: %s", encoded)
	}
}

func TestMemoryUnavailableFieldsRemainIndependent(t *testing.T) {
	result := readOSMemory("linux", func(path string) ([]byte, error) {
		if path == "/proc/self/status" {
			return []byte("VmRSS: 1 kB\nVmSwap: 2 kB\n"), nil
		}
		return []byte("MemTotal: 1000 kB\n"), nil
	})
	if !result.ProcessAvailable || result.SystemAvailable || result.SystemTotalBytes != nil || result.SystemError == "" {
		t.Fatalf("missing system field not diagnosed independently: %+v", result)
	}
	unsupported := readOSMemory("darwin", func(string) ([]byte, error) {
		t.Fatal("unsupported platform attempted procfs read")
		return nil, nil
	})
	if unsupported.ProcessAvailable || unsupported.SystemAvailable || unsupported.ProcessError != "unsupported_platform" {
		t.Fatalf("unexpected platform result: %+v", unsupported)
	}
}

func TestProcMemoryRejectsMalformedRequiredFields(t *testing.T) {
	for name, data := range map[string]string{
		"missing": "Name: process\n", "bad unit": "VmRSS: 10 MB\n",
		"negative": "VmRSS: -1 kB\n", "overflow": "VmRSS: 18446744073709551615 kB\n",
		"duplicate": "VmRSS: 1 kB\nVmRSS: 2 kB\n",
	} {
		t.Run(name, func(t *testing.T) {
			if _, err := parseProcKB([]byte(data), "VmRSS"); err == nil {
				t.Fatal("malformed memory value accepted")
			}
		})
	}
}

func TestLogMemoryProducesOneStructuredSample(t *testing.T) {
	var buffer bytes.Buffer
	logger := log.New(&buffer, "", 0)
	LogMemory(logger, "candidate_planning_end", 41)
	line := buffer.String()
	if strings.Count(line, "\n") != 1 || !strings.HasPrefix(line, "SUPERVISOR_MEMORY ") {
		t.Fatalf("unexpected memory line: %s", line)
	}
	var sample MemorySample
	if err := json.Unmarshal([]byte(strings.TrimPrefix(line, "SUPERVISOR_MEMORY ")), &sample); err != nil {
		t.Fatal(err)
	}
	if sample.SchemaVersion != 1 || sample.Stage != "candidate_planning_end" || sample.Epoch != 41 || sample.PID <= 0 || sample.UnixNS <= 0 || sample.HeapInuse == 0 || sample.Goroutines <= 0 || sample.OS.Platform == "" {
		t.Fatalf("incomplete sample: %+v", sample)
	}
	LogMemory(nil, "ignored", 0)
}

func TestMemoryObserverStopsOnceAndEmitsNoLaterSamples(t *testing.T) {
	var mu sync.Mutex
	var stages []string
	periodic := make(chan struct{}, 1)
	stop := startMemoryObserver(func(stage string) {
		mu.Lock()
		stages = append(stages, stage)
		mu.Unlock()
		if stage == "periodic" {
			select {
			case periodic <- struct{}{}:
			default:
			}
		}
	}, time.Millisecond)
	select {
	case <-periodic:
	case <-time.After(time.Second):
		stop()
		t.Fatal("periodic observer did not run")
	}
	var wait sync.WaitGroup
	for i := 0; i < 4; i++ {
		wait.Add(1)
		go func() { defer wait.Done(); stop() }()
	}
	wait.Wait()
	mu.Lock()
	count := len(stages)
	if count < 3 || stages[0] != "startup" || stages[count-1] != "shutdown" {
		t.Fatalf("unexpected observer lifetime: %v", stages)
	}
	shutdowns := 0
	for _, stage := range stages {
		if stage == "shutdown" {
			shutdowns++
		}
	}
	mu.Unlock()
	time.Sleep(5 * time.Millisecond)
	mu.Lock()
	defer mu.Unlock()
	if shutdowns != 1 || len(stages) != count {
		t.Fatalf("observer did not stop exactly once: %v", stages)
	}
	StartMemoryObserver(nil, time.Millisecond)()
}
