package committee

import (
	"blockEmulator/core"
	"log"
	"math/big"
	"strconv"
	"strings"
	"time"
)

// transform one csv row to a tx shared by all committee modes.
func data2tx(data []string, nonce uint64) (*core.Transaction, bool) {
	if len(data) <= 8 {
		return &core.Transaction{}, false
	}
	if data[6] == "0" && data[7] == "0" && len(data[3]) > 16 && len(data[4]) > 16 && data[3] != data[4] {
		val, ok := new(big.Int).SetString(data[8], 10)
		if !ok {
			log.Panic("new int failed\n")
		}
		tx := core.NewTransaction(data[3][2:], data[4][2:], val, nonce, time.Now())
		tx.GasUsed = parseGasUsed(data)
		tx.ExecDelayMs = core.ComputeTxExecDelayMs(tx.GasUsed)
		return tx, true
	}
	return &core.Transaction{}, false
}

func parseGasUsed(data []string) uint64 {
	if len(data) <= 11 {
		return 0
	}
	raw := strings.TrimSpace(data[11])
	if raw == "" {
		return 0
	}
	gasUsed, err := strconv.ParseUint(raw, 10, 64)
	if err != nil {
		return 0
	}
	return gasUsed
}
