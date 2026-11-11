package build

import (
	"blockEmulator/params"
	"fmt"
	"runtime"
	"sort"
	"strings"
)

func GenerateBatchByIpTable(nodenum, shardnum int) error {
	if runtime.GOOS != "windows" {
		return generateManagedUnixBatch(nodenum, shardnum, false)
	}
	// read IP table file first
	ipMap := readIpTable(params.IpTableFile)

	// determine the formats of commands and fileNames, according to operating system
	var fileNameFormat, commandFormat string
	os := runtime.GOOS
	switch os {
	case "windows":
		fileNameFormat = "complie_run_IpAddr=%s.bat"
		commandFormat = "start cmd /k go run main.go"
	default:
		fileNameFormat = "complie_run_IpAddr=%s.sh"
		commandFormat = "go run main.go"
	}

	// generate file for each ip
	for i := 0; i < shardnum; i++ {
		// if this shard is not existed, return
		if _, shard_exist := ipMap[uint64(i)]; !shard_exist {
			return fmt.Errorf("the shard (shardID = %d) is not existed in the IP Table file", i)
		}
		// if this shard is existed.
		for j := 0; j < nodenum; j++ {
			if nodeIp, node_exist := ipMap[uint64(i)][uint64(j)]; node_exist {
				// attach this command to this file
				ipAddr := strings.Split(nodeIp, ":")[0]
				batFilePath := fmt.Sprintf(fileNameFormat, strings.ReplaceAll(ipAddr, ".", "_"))
				command := fmt.Sprintf(commandFormat+" -n %d -N %d -s %d -S %d & \n", j, nodenum, i, shardnum)
				if err := attachLineToFile(batFilePath, command); nil != err {
					return err
				}
			} else {
				return fmt.Errorf("the node (shardID = %d, nodeID = %d) is not existed in the IP Table file", i, j)
			}
		}
	}

	// generate command for supervisor
	if supervisorShard, shard_exist := ipMap[2147483647]; shard_exist {
		if nodeIp, node_exist := supervisorShard[0]; node_exist {
			ipAddr := strings.Split(nodeIp, ":")[0]
			batFilePath := fmt.Sprintf(fileNameFormat, strings.ReplaceAll(ipAddr, ".", "_"))
			supervisor_command := fmt.Sprintf(commandFormat+" -c -N %d -S %d & \n", nodenum, shardnum)
			if err := attachLineToFile(batFilePath, supervisor_command); nil != err {
				return err
			}
			return nil
		}
	}
	return fmt.Errorf("the supervisor (shardID = 2147483647, nodeID = 0) is not existed in the IP Table file")
}

func GenerateExeBatchByIpTable(nodenum, shardnum int) error {
	if runtime.GOOS != "windows" {
		return generateManagedUnixBatch(nodenum, shardnum, true)
	}
	// read IP table file first
	ipMap := readIpTable(params.IpTableFile)

	// determine the formats of commands and fileNames, according to operating system
	var fileNameFormat, commandFormat string
	os := runtime.GOOS
	switch os {
	case "windows":
		fileNameFormat = os + "_exe_run_IpAddr=%s.bat"
		commandFormat = "start cmd /k blockEmulator_Windows_Precompile.exe"
	default:
		fileNameFormat = os + "_exe_run_IpAddr=%s.sh"
		commandFormat = "./blockEmulator_" + os + "_Precompile"
	}

	// generate file for each ip
	for i := 0; i < shardnum; i++ {
		// if this shard is not existed, return
		if _, shard_exist := ipMap[uint64(i)]; !shard_exist {
			return fmt.Errorf("the shard (shardID = %d) is not existed in the IP Table file", i)
		}
		// if this shard is existed.
		for j := 0; j < nodenum; j++ {
			if nodeIp, node_exist := ipMap[uint64(i)][uint64(j)]; node_exist {
				// attach this command to this file
				ipAddr := strings.Split(nodeIp, ":")[0]
				batFilePath := fmt.Sprintf(fileNameFormat, strings.ReplaceAll(ipAddr, ".", "_"))
				command := fmt.Sprintf(commandFormat+" -n %d -N %d -s %d -S %d & \n", j, nodenum, i, shardnum)
				if err := attachLineToFile(batFilePath, command); nil != err {
					return err
				}
			} else {
				return fmt.Errorf("the node (shardID = %d, nodeID = %d) is not existed in the IP Table file", i, j)
			}
		}
	}

	// generate command for supervisor
	if supervisorShard, shard_exist := ipMap[2147483647]; shard_exist {
		if nodeIp, node_exist := supervisorShard[0]; node_exist {
			ipAddr := strings.Split(nodeIp, ":")[0]
			batFilePath := fmt.Sprintf(fileNameFormat, strings.ReplaceAll(ipAddr, ".", "_"))
			supervisor_command := fmt.Sprintf(commandFormat+" -c -N %d -S %d & \n", nodenum, shardnum)
			if err := attachLineToFile(batFilePath, supervisor_command); nil != err {
				return err
			}
			return nil
		}
	}
	return fmt.Errorf("the supervisor (shardID = 2147483647, nodeID = 0) is not existed in the IP Table file")
}

// The subshell owns exactly the PIDs it creates, so sourcing a generated file
// does not leave traps or shell options behind in the user's interactive shell.
// A fatal exit also exits the sourcing shell: this preserves a failure status
// for the documented `bash -c 'source launcher.sh; wait'` invocation.
const managedLauncherHeader = `#!/usr/bin/env bash
(
set -u
be_pids=()
be_roles=()
be_cleanup() {
  local pid tick alive
  for pid in "${be_pids[@]}"; do
    [[ -z "$pid" ]] || kill -TERM "$pid" 2>/dev/null || true
  done
  for ((tick=0; tick<50; tick++)); do
    alive=0
    for pid in "${be_pids[@]}"; do
      if [[ -n "$pid" ]] && kill -0 "$pid" 2>/dev/null; then alive=1; fi
    done
    [[ "$alive" == 1 ]] || break
    sleep 0.1
  done
  for pid in "${be_pids[@]}"; do
    [[ -z "$pid" ]] || kill -KILL "$pid" 2>/dev/null || true
    [[ -z "$pid" ]] || wait "$pid" 2>/dev/null || true
  done
}
trap be_cleanup EXIT
trap 'exit 130' INT
trap 'exit 143' TERM
be_launch() {
  local role="$1"
  shift
  "$@" &
  be_pids+=("$!")
  be_roles+=("$role")
}
`

const managedLauncherFooter = `
be_deadline=0
while :; do
  be_alive=0
  for ((be_i=0; be_i<${#be_pids[@]}; be_i++)); do
    be_pid="${be_pids[$be_i]}"
    [[ -n "$be_pid" ]] || continue
    if kill -0 "$be_pid" 2>/dev/null; then
      be_alive=$((be_alive+1))
      continue
    fi
    be_status=0
    wait "$be_pid" || be_status=$?
    be_pids[$be_i]=''
    if [[ "$be_status" != 0 ]]; then
      echo "Experiment failed: ${be_roles[$be_i]} (PID $be_pid) exited $be_status; stopping this run's remaining processes." >&2
      exit "$be_status"
    fi
    # Stop ACKs mean shutdown has started, not that the processes have exited.
    # A first clean worker exit also bounds a supervisor stuck while finishing.
    if [[ "$be_deadline" == 0 ]]; then be_deadline=$((SECONDS+60)); fi
  done
  [[ "$be_alive" != 0 ]] || break
  if [[ "$be_deadline" != 0 && "$SECONDS" -ge "$be_deadline" ]]; then
    echo 'Experiment shutdown timed out; stopping only the remaining PIDs from this run.' >&2
    exit 1
  fi
  sleep 0.2
done
echo 'Experiment processes all exited successfully.'
) || exit $?
`

func generateManagedUnixBatch(nodenum, shardnum int, precompiled bool) error {
	ipMap := readIpTable(params.IpTableFile)
	commands := make(map[string][]string)
	binary := "./blockEmulator_" + runtime.GOOS + "_Precompile"
	for sid := 0; sid < shardnum; sid++ {
		for nid := 0; nid < nodenum; nid++ {
			address, exists := ipMap[uint64(sid)][uint64(nid)]
			if !exists {
				return fmt.Errorf("node S%dN%d missing from IP table", sid, nid)
			}
			ip := strings.Split(address, ":")[0]
			commands[ip] = append(commands[ip], fmt.Sprintf("be_launch S%dN%d %s -n %d -N %d -s %d -S %d", sid, nid, binary, nid, nodenum, sid, shardnum))
		}
	}
	address, exists := ipMap[params.SupervisorShard][0]
	if !exists {
		return fmt.Errorf("supervisor missing from IP table")
	}
	ip := strings.Split(address, ":")[0]
	commands[ip] = append(commands[ip], fmt.Sprintf("be_launch Supervisor %s -c -N %d -S %d", binary, nodenum, shardnum))
	ips := make([]string, 0, len(commands))
	for ip := range commands {
		ips = append(ips, ip)
	}
	sort.Strings(ips)
	for _, ip := range ips {
		prefix := runtime.GOOS + "_exe_run"
		setup := ""
		if !precompiled {
			prefix = "complie_run"
			// Launch the binary directly, so tracked PIDs are the experiment
			// processes rather than `go run` wrappers with untracked children.
			setup = "go build -o " + binary + " . || exit $?\n"
		}
		path := fmt.Sprintf("%s_IpAddr=%s.sh", prefix, strings.ReplaceAll(ip, ".", "_"))
		if err := attachLineToFile(path, managedLauncherHeader+setup+strings.Join(commands[ip], "\n")+managedLauncherFooter); err != nil {
			return err
		}
	}
	return nil
}
