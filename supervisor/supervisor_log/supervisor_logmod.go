package supervisor_log

import (
	"blockEmulator/params"
	"io"
	"log"
	"os"
)

type SupervisorLog struct {
	Slog *log.Logger
}

func NewSupervisorLog() *SupervisorLog {
	writer1 := os.Stdout

	dirpath := params.LogWrite_path
	err := os.MkdirAll(dirpath, os.ModePerm)
	if err != nil {
		log.Panic(err)
	}
	writer2, err := os.OpenFile(dirpath+"/Supervisor.log", os.O_WRONLY|os.O_CREATE, 0755)
	if err != nil {
		log.Panic(err)
	}
	runtimeWriter, err := os.OpenFile(dirpath+"/Supervisor_runtime.log", os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0644)
	if err != nil {
		log.Panic(err)
	}
	// Most supervisor code uses the package-level logger for fatal errors. Keep
	// those diagnostics in the experiment directory while preserving stderr.
	// Fatal runtime errors and SIGKILL cannot be intercepted from inside the
	// process; the GLAS memory heartbeat still shows the last observed state.
	log.SetOutput(io.MultiWriter(os.Stderr, runtimeWriter))
	pl := log.New(io.MultiWriter(writer1, writer2), "Supervisor: ", log.Lshortfile|log.Ldate|log.Ltime)
	return &SupervisorLog{
		Slog: pl,
	}
}
