package main

import (
	"bufio"
	"crypto/sha256"
	"encoding/csv"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"blockEmulator/chain"
	"blockEmulator/core"
	"blockEmulator/glasfreight/model"
	"blockEmulator/message"
	"blockEmulator/params"
	"blockEmulator/partition/garet"
	"blockEmulator/workload"
)

const txHeader = "blockNumber,transactionIndex,transactionHash,from,to,toCreate,value,gasUsed,receiptStatus,fromIsContract,toIsContract,txCategory\n"
const callHeader = "blockNumber,transactionIndex,transactionHash,traceAddress,callDepth,caller,callee,callType,callGasUsed,error,txStatus,callerIsContract,calleeIsContract,isCCGEdge\n"

func TestFrozenMapUsesNextAdmissibleCohortAndPhysicalGas(t *testing.T) {
	old := params.ShardNum
	t.Cleanup(func() { params.ShardNum = old })
	a, b, c := fmt.Sprintf("%040x", 0), fmt.Sprintf("%040x", 1), fmt.Sprintf("%040x", 2)
	txs := txHeader
	for i, rec := range []struct {
		to string
		ok int
	}{{c, 0}, {b, 1}, {c, 1}, {b, 1}, {b, 1}} {
		txs += fmt.Sprintf("1,%d,0x%064x,0x%s,0x%s,,0,40,%d,false,true,inter_contract_call\n", i, i+1, a, rec.to, rec.ok)
	}
	calls := callHeader
	for _, i := range []int{2, 3} {
		calls += fmt.Sprintf("1,%d,0x%064x,0,1,0x%s,0x%s,call,10,,1,true,true,true\n", i, i+1, a, b)
	}
	var ordered []byte
	for _, i := range []int{3, 4} {
		hash, _ := hex.DecodeString(fmt.Sprintf("%064x", i))
		ordered = append(ordered, hash...)
	}
	cohort := fmt.Sprintf("%x", sha256.Sum256(ordered))
	for _, tc := range []struct {
		name               string
		overrides          map[string]uint64
		wantTop, wantUnion uint64
		gas                []uint64
	}{
		{"hash", map[string]uint64{}, 1, 2, []uint64{85, 45}},
		{"co_located", map[string]uint64{b: 0}, 0, 0, []uint64{80, 0}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			reader, err := workload.NewReader(strings.NewReader(txs), strings.NewReader(calls), 100)
			if err != nil {
				t.Fatal(err)
			}
			defer reader.Close()
			p := checkpoint{Position: 1, Epoch: 7, Shards: 2, Method: 1, Overrides: tc.overrides}
			res, err := p.resolver(nil)
			if err != nil {
				t.Fatal(err)
			}
			m, err := evaluate(reader, p, config{TotalDataSize: 5, BlockGasLimit: 100, UnifiedCrossCallGas: 5}, 2, res)
			if err != nil {
				t.Fatal(err)
			}
			if m.count != 2 || m.cohort != cohort || m.top != tc.wantTop || m.union != tc.wantUnion || !reflect.DeepEqual(m.shardGas, tc.gas) {
				t.Fatalf("wrong fixed cohort or physical cost: %+v", m)
			}
			b, err := m.csv(runState{}, config{}, p)
			if err != nil {
				t.Fatal(err)
			}
			rows, err := csv.NewReader(strings.NewReader(string(b))).ReadAll()
			if err != nil || len(rows) != 2 || len(rows[0]) != len(rows[1]) {
				t.Fatalf("invalid output %s %v", b, err)
			}
		})
	}
}
func TestGaretNeedsCertifiedGroupPlacement(t *testing.T) {
	a, b := fmt.Sprintf("%040x", 0), fmt.Sprintf("%040x", 1)
	c, err := garet.NewCatalog([]string{a, b}, 2)
	if err != nil {
		t.Fatal(err)
	}
	p := checkpoint{Epoch: 4, Shards: 2, Method: 10, GARET: &message.GARETPlacement{CatalogDigest: c.Digest(), Epoch: 4, Placement: []uint64{1, 0}}}
	res, err := p.resolver(c)
	if err != nil {
		t.Fatal(err)
	}
	s, err := res(a)
	if s != 1 || err != nil {
		t.Fatalf("group routing: got %v %v", s, err)
	}
	p.GARET.Epoch--
	if _, err := p.resolver(c); err == nil {
		t.Fatal("accepted mismatched GARET epoch")
	}
	p.GARET.Epoch++
	p.GARET.CatalogDigest = "wrong"
	if _, err := p.resolver(c); err == nil {
		t.Fatal("accepted mismatched GARET digest")
	}
	p.GARET.CatalogDigest = c.Digest()
	p.Overrides = map[string]uint64{a: 0}
	if _, err := p.resolver(c); err == nil {
		t.Fatal("accepted mixed GARET/override routing")
	}
}
func TestIdempotenceAndFingerprint(t *testing.T) {
	path := filepath.Join(t.TempDir(), "out.csv")
	if err := publish(path, []byte("a")); err != nil {
		t.Fatal(err)
	}
	if err := publish(path, []byte("a")); err != nil {
		t.Fatal(err)
	}
	if err := publish(path, []byte("new")); err == nil {
		t.Fatal("overwrote existing results")
	}
	if got, err := os.ReadFile(path); err != nil || string(got) != "a" {
		t.Fatalf("modified original: %s; %v", got, err)
	}
	if err := checkFile(path, fmt.Sprintf("%x", sha256.Sum256([]byte("a")))); err != nil {
		t.Fatal(err)
	}
	if err := checkFile(path, fmt.Sprintf("%x", sha256.Sum256([]byte("b")))); err == nil {
		t.Fatal("accepted changed input")
	}
}
func TestSingleStageCannotExceedCapacity(t *testing.T) {
	m := metrics{shardGas: []uint64{2, 3}}
	if err := m.add(2, 45, 60); err == nil {
		t.Fatal("accepted unknown shard")
	}
	if err := m.add(0, 61, 60); err == nil {
		t.Fatal("accepted overlimit stage")
	}
}

// Exercise the standalone CLI path with its actual 50k-row horizon, without a simulator.
func TestFullOfflineFixture(t *testing.T) {
	const n = 100_000
	old := params.ShardNum
	t.Cleanup(func() { params.ShardNum = old })
	dir := t.TempDir()
	var s runState
	s.Status = "completed"
	s.RunID = "fixture"
	s.Files = map[string]string{}
	s.Fingerprint.Shards = 2
	s.Fingerprint.Data = map[string]string{}
	c := config{ConsensusMethod: 1, TotalDataSize: n, FormalMapCheckpoint: n / 2, BlockGasLimit: 100, UnifiedCrossCallGas: 3, UnifiedMetrics: true, UnifiedExecution: true, UnifiedTransactionsFile: filepath.Join(dir, "tx.csv"), UnifiedCallsFile: filepath.Join(dir, "call.csv")}
	a, b := fmt.Sprintf("%040x", 2), fmt.Sprintf("%040x", 3)
	tx, err := os.Create(c.UnifiedTransactionsFile)
	if err != nil {
		t.Fatal(err)
	}
	w := bufio.NewWriter(tx)
	fmt.Fprint(w, txHeader)
	identity := sha256.New()
	for i := 1; i <= n; i++ {
		if _, err = fmt.Fprintf(w, "1,%d,0x%064x,0x%s,0x%s,,0,40,1,false,true,inter_contract_call\n", i, i, a, b); err != nil {
			t.Fatal(err)
		}
		if i > n/2 {
			v, _ := hex.DecodeString(fmt.Sprintf("%064x", i))
			identity.Write(v)
		}
	}
	if err = w.Flush(); err != nil {
		t.Fatal(err)
	}
	if err = tx.Close(); err != nil {
		t.Fatal(err)
	}
	if err = os.WriteFile(c.UnifiedCallsFile, []byte(callHeader), 0600); err != nil {
		t.Fatal(err)
	}
	for name, path := range map[string]string{"UnifiedTransactionsFile": c.UnifiedTransactionsFile, "UnifiedCallsFile": c.UnifiedCallsFile} {
		data, err := os.ReadFile(path)
		if err != nil {
			t.Fatal(err)
		}
		s.Fingerprint.Data[name] = fmt.Sprintf("%x", sha256.Sum256(data))
	}
	for name, obj := range map[string]interface{}{
		"runtime_config.json":       c,
		"analysis/summary.json":     map[string]interface{}{"valid_for_publication": true, "full_run": map[string]interface{}{"complete": true, "dispatched_unique": n, "completed_unique": n}},
		"analysis/validation.json":  map[string]bool{"valid_for_publication": true},
		"result/e1_checkpoint.json": checkpoint{Position: n / 2, Shards: 2, Epoch: 3, Method: 1, Overrides: map[string]uint64{}},
	} {
		path := filepath.Join(dir, name)
		if err = os.MkdirAll(filepath.Dir(path), 0755); err != nil {
			t.Fatal(err)
		}
		data, err := json.Marshal(obj)
		if err != nil {
			t.Fatal(err)
		}
		if err = os.WriteFile(path, data, 0644); err != nil {
			t.Fatal(err)
		}
		s.Files[name] = fmt.Sprintf("%x", sha256.Sum256(data))
	}
	data, err := json.Marshal(s)
	if err != nil {
		t.Fatal(err)
	}
	if err = os.WriteFile(filepath.Join(dir, "run_state.json"), data, 0644); err != nil {
		t.Fatal(err)
	}
	out := filepath.Join(t.TempDir(), "checkpoint_metrics.csv")
	if err = run(dir, out); err != nil {
		t.Fatal(err)
	}
	data, err = os.ReadFile(out)
	if err != nil {
		t.Fatal(err)
	}
	rows, err := csv.NewReader(strings.NewReader(string(data))).ReadAll()
	if err != nil || len(rows) != 2 {
		t.Fatalf("bad CSV: %v", err)
	}
	fields := make(map[string]string)
	for i, name := range rows[0] {
		fields[name] = rows[1][i]
	}
	if fields["source_position"] != "50000" || fields["evaluation_count"] != "50000" || fields["cohort_sha256"] != fmt.Sprintf("%x", identity.Sum(nil)) || fields["top_cross_count"] != "50000" || fields["per_shard_gas"] != "[2000000,2000000]" || fields["peak_to_mean"] != "1" {
		t.Fatalf("unexpected summary: %v", fields)
	}
	if err = os.WriteFile(c.UnifiedCallsFile, []byte("not the verified table"), 0600); err != nil {
		t.Fatal(err)
	}
	if err = run(dir, out); err == nil {
		t.Fatal("accepted changed input")
	}
}

// Check the offline pricing against the exact resolver and pricing entry points used by a node.
func TestParityWithActualExecutionRouting(t *testing.T) {
	old := params.ShardNum
	params.ShardNum = 4
	t.Cleanup(func() { params.ShardNum = old })
	addrs := make([]string, 8)
	for i := range addrs {
		addrs[i] = fmt.Sprintf("%040x", i+1)
	}
	cat, err := garet.NewCatalog(addrs, 4)
	if err != nil {
		t.Fatal(err)
	}
	for _, method := range []int{1, 8, 9, 10, 12} {
		node := &chain.BlockChain{ChainConfig: &params.ChainConfig{ShardNums: 4}, PartitionMap: map[string]uint64{}}
		cp := checkpoint{Shards: 4, Method: method, Epoch: 1}
		if method == 10 {
			if err = node.InitGARETRouting(cat); err != nil {
				t.Fatal(err)
			}
			groups := []uint64{2, 1, 3, 0}
			if err = node.InstallGARETPlacement(cat.Digest(), 1, groups, nil); err != nil {
				t.Fatal(err)
			}
			cp.GARET = &message.GARETPlacement{Epoch: 1, CatalogDigest: cat.Digest(), Placement: groups}
		} else {
			cp.Overrides = map[string]uint64{addrs[0]: 3, addrs[7]: 1}
			for a, shard := range cp.Overrides {
				node.Update_PartitionMap(a, shard)
			}
		}
		resolve, err := cp.resolver(cat)
		if err != nil {
			t.Fatal(err)
		}
		for i, a := range addrs {
			s, err := resolve(a)
			if err != nil || s != node.Get_PartitionMap(a) {
				t.Fatalf("%d: routing %s", method, a)
			}
			b := addrs[(i+1)%len(addrs)]
			rec := &model.Transaction{SourceHash: fmt.Sprintf("0x%064x", i+1), From: a, To: b, Value: "0", GasUsed: 40, HasGasUsed: true,
				Calls: []model.Call{{Caller: addrs[2], Callee: addrs[4], IsCCGEdge: true, CallerIsSCA: true, CalleeIsSCA: true}}}
			m := metrics{shardGas: make([]uint64, 4)}
			if err = m.price(rec, uint64(i+1), cp.Epoch, 100, 3, resolve); err != nil {
				t.Fatal(err)
			}
			tx, err := workload.CoreTransaction(rec, uint64(i), 3)
			if err != nil {
				t.Fatal(err)
			}
			if err = core.PriceExecutionStage(tx, node.Get_PartitionMap); err != nil {
				t.Fatal(err)
			}
			src, dst := node.Get_PartitionMap(a), node.Get_PartitionMap(b)
			want := make([]uint64, 4)
			want[src] += tx.GasUsed
			if src != dst {
				relay, err := core.NewExecutionRelay2(tx, cp.Epoch, node.Get_PartitionMap)
				if err != nil {
					t.Fatal(err)
				}
				want[dst] += relay.GasUsed
			}
			if !reflect.DeepEqual(want, m.shardGas) || (m.top == 1) != (src != dst) || (m.union == 1) != (src != dst || tx.GLASCrossCallCount != 0) {
				t.Fatalf("%d %s->%s: %+v want %v", method, a, b, m, want)
			}
		}
	}
}
