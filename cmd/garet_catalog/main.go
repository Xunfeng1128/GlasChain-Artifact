// garet_catalog extracts only identities from an admitted workload prefix. It
// does not replay transactions, build a graph, or inspect future load frequency.
package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"

	"blockEmulator/partition/garet"
	"blockEmulator/workload"
)

func main() { os.Exit(run(os.Args[1:], os.Stdout, os.Stderr)) }

func run(args []string, stdout, stderr io.Writer) int {
	flags := flag.NewFlagSet("garet_catalog", flag.ContinueOnError)
	flags.SetOutput(stderr)
	transactions := flags.String("transactions", "", "canonical transactions CSV path")
	calls := flags.String("calls", "", "canonical contract calls CSV path")
	count := flags.Uint64("count", 0, "number of admitted prefix transactions whose identities are included")
	limit := flags.Uint64("block-gas-limit", 0, "unified admission Gas limit")
	groups := flags.Int("groups", garet.DefaultGroups, "number of fixed nonempty account groups")
	output := flags.String("output", "", "output JSON catalog path")
	if err := flags.Parse(args); err != nil {
		return 2
	}
	fail := func(code int, err error) int { fmt.Fprintf(stderr, "garet_catalog: %v\n", err); return code }
	if flags.NArg() != 0 {
		return fail(2, fmt.Errorf("unexpected positional arguments"))
	}
	if strings.TrimSpace(*transactions) == "" || strings.TrimSpace(*calls) == "" || strings.TrimSpace(*output) == "" || *count == 0 || *limit == 0 || *groups <= 0 {
		return fail(2, fmt.Errorf("--transactions, --calls, --output, positive --count, --block-gas-limit and --groups are required"))
	}
	for _, input := range []string{*transactions, *calls} {
		inputAbs, err := filepath.Abs(input)
		if err != nil {
			return fail(2, err)
		}
		outputAbs, err := filepath.Abs(*output)
		if err != nil {
			return fail(2, err)
		}
		inputInfo, inputErr := os.Stat(input)
		outputInfo, outputErr := os.Stat(*output)
		if inputAbs == outputAbs || (inputErr == nil && outputErr == nil && os.SameFile(inputInfo, outputInfo)) {
			return fail(2, fmt.Errorf("output must not replace an input file"))
		}
	}
	reader, err := workload.Open(*transactions, *calls, *limit)
	if err != nil {
		return fail(1, err)
	}
	defer reader.Close()
	identities := make(map[string]struct{})
	add := func(address string) error {
		normalized, err := garet.NormalizeAddress(address)
		if err != nil {
			return err
		}
		identities[normalized] = struct{}{}
		return nil
	}
	for index := uint64(0); index < *count; index++ {
		record, err := reader.Next()
		if err == io.EOF {
			return fail(1, fmt.Errorf("workload ended after %d admitted transactions; requested %d", index, *count))
		}
		if err != nil {
			return fail(1, fmt.Errorf("admitted transaction %d: %w", index+1, err))
		}
		if err := add(record.From); err != nil {
			return fail(1, err)
		}
		if err := add(record.To); err != nil {
			return fail(1, err)
		}
		for _, call := range record.Calls {
			if call.Caller != "" {
				if err := add(call.Caller); err != nil {
					return fail(1, err)
				}
			}
			if call.Callee != "" {
				if err := add(call.Callee); err != nil {
					return fail(1, err)
				}
			}
		}
	}
	addresses := make([]string, 0, len(identities))
	for address := range identities {
		addresses = append(addresses, address)
	}
	catalog, err := garet.NewCatalog(addresses, *groups)
	if err != nil {
		return fail(1, err)
	}
	if err := garet.SaveCatalog(*output, catalog); err != nil {
		return fail(1, err)
	}
	summary := struct {
		CatalogDigest      string `json:"catalog_digest"`
		GroupCount         int    `json:"group_count"`
		AddressCount       uint64 `json:"address_count"`
		PrefixTransactions uint64 `json:"prefix_transactions"`
		Output             string `json:"output"`
	}{catalog.Digest(), catalog.GroupCount(), catalog.AddressCount(), *count, *output}
	if err := json.NewEncoder(stdout).Encode(summary); err != nil {
		return fail(2, err)
	}
	return 0
}
