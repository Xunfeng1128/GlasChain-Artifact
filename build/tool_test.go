package build

import (
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestManagedLauncherFailureCleansOnlyChildren(t *testing.T) {
	if _, err := exec.LookPath("bash"); err != nil {
		t.Skip("bash unavailable")
	}
	dir := t.TempDir()
	pidPath := filepath.Join(dir, "worker.pid")
	body := managedLauncherHeader + `
be_launch worker bash -c 'echo $$ > "$PID_FILE"; exec sleep 30'
be_launch supervisor bash -c 'sleep 0.1; exit 7'
` + managedLauncherFooter
	launcherPath := filepath.Join(dir, "launcher.sh")
	if err := os.WriteFile(launcherPath, []byte(body), 0600); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	// The trailing wait must not replace the launcher's failure with success.
	cmd := exec.CommandContext(ctx, "bash", "-c", `source "$LAUNCHER_FILE"; wait`)
	cmd.Env = append(os.Environ(), "PID_FILE="+pidPath, "LAUNCHER_FILE="+launcherPath)
	output, err := cmd.CombinedOutput()
	if err == nil || ctx.Err() != nil {
		t.Fatalf("expected prompt failure, err=%v context=%v output=%s", err, ctx.Err(), output)
	}
	if exit, ok := err.(*exec.ExitError); !ok || exit.ExitCode() != 7 {
		t.Fatalf("source + wait lost failure status: %v", err)
	}
	pid, err := os.ReadFile(pidPath)
	if err != nil {
		t.Fatal(err)
	}
	if err := exec.Command("kill", "-0", strings.TrimSpace(string(pid))).Run(); err == nil {
		t.Fatalf("managed worker remains alive: %s", pid)
	}
}

func TestManagedLauncherSuccessfulCompletion(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	body := managedLauncherHeader + "be_launch worker true\nbe_launch supervisor true\n" + managedLauncherFooter
	if output, err := exec.CommandContext(ctx, "bash", "-c", body).CombinedOutput(); err != nil {
		t.Fatalf("err=%v output=%s", err, output)
	}
}
