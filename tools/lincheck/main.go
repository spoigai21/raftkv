// lincheck checks recorded raftkv histories for linearizability with Porcupine.
//
//	lincheck [-timeout 30s] [-viz DIR] history.json...
//
// Each file is one run: every client operation with its call and return time. An
// operation that never returned (its client was still waiting when the run stopped) has
// "return": null. It may or may not have taken effect, so it is given a return time after
// everything else and any output, which lets it linearize anywhere after its call, or,
// in effect, not at all (implementation guide Phase 6).
//
// This is a development tool, written in Go because Porcupine is. The C++ project does not
// depend on it; tests/lincheck/run.sh builds and runs it.
//
// Exit codes: 0 all linearizable, 1 some history is not, 2 a check timed out or bad input.
package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"github.com/anishathalye/porcupine"
)

type recordedOp struct {
	Client uint64  `json:"client"`
	Op     string  `json:"op"` // get, put, append
	Key    string  `json:"key"`
	Value  string  `json:"value"`
	Call   int64   `json:"call"`
	Return *int64  `json:"return"` // null: never returned
	Output *output `json:"output"` // null: never returned
}

type output struct {
	Value string `json:"value"`
	Found bool   `json:"found"`
}

type history struct {
	Seed    uint64       `json:"seed"`
	Variant string       `json:"variant"`
	Ops     []recordedOp `json:"ops"`
}

type kvInput struct {
	Op, Key, Value string
}

type kvOutput struct {
	Value   string
	Found   bool
	Unknown bool // the operation never returned; any outcome is acceptable
}

type kvState struct {
	Value string
	Found bool
}

var kvModel = porcupine.Model{
	// Keys are independent, so each key's history is checked on its own: much faster.
	Partition: func(ops []porcupine.Operation) [][]porcupine.Operation {
		byKey := map[string][]porcupine.Operation{}
		for _, op := range ops {
			k := op.Input.(kvInput).Key
			byKey[k] = append(byKey[k], op)
		}
		keys := make([]string, 0, len(byKey))
		for k := range byKey {
			keys = append(keys, k)
		}
		sort.Strings(keys)
		out := make([][]porcupine.Operation, 0, len(keys))
		for _, k := range keys {
			out = append(out, byKey[k])
		}
		return out
	},
	Init: func() interface{} { return kvState{} },
	Step: func(state, input, out interface{}) (bool, interface{}) {
		s := state.(kvState)
		in := input.(kvInput)
		o := out.(kvOutput)
		switch in.Op {
		case "get":
			return o.Unknown || (o.Found == s.Found && o.Value == s.Value), s
		case "put":
			return true, kvState{Value: in.Value, Found: true}
		case "append":
			return true, kvState{Value: s.Value + in.Value, Found: true}
		}
		return false, s
	},
	Equal: func(a, b interface{}) bool { return a.(kvState) == b.(kvState) },
	DescribeOperation: func(input, out interface{}) string {
		in := input.(kvInput)
		o := out.(kvOutput)
		switch {
		case o.Unknown:
			return fmt.Sprintf("%s(%q, %q) -> ?", in.Op, in.Key, in.Value)
		case in.Op == "get" && !o.Found:
			return fmt.Sprintf("get(%q) -> not found", in.Key)
		case in.Op == "get":
			return fmt.Sprintf("get(%q) -> %q", in.Key, o.Value)
		}
		return fmt.Sprintf("%s(%q, %q)", in.Op, in.Key, in.Value)
	},
	DescribeState: func(state interface{}) string {
		s := state.(kvState)
		if !s.Found {
			return "<absent>"
		}
		return fmt.Sprintf("%q", s.Value)
	},
}

// toOperations converts a recorded history, numbering clients from zero (the visualizer
// needs that) and giving never-returned operations a return time after everything else.
func toOperations(h history) ([]porcupine.Operation, int, error) {
	var last int64
	clients := map[uint64]int{}
	for _, r := range h.Ops {
		if r.Return != nil && *r.Return > last {
			last = *r.Return
		}
		if r.Call > last {
			last = r.Call
		}
		if _, ok := clients[r.Client]; !ok {
			clients[r.Client] = len(clients)
		}
	}
	ops := make([]porcupine.Operation, 0, len(h.Ops))
	pending := 0
	for i, r := range h.Ops {
		switch r.Op {
		case "get", "put", "append":
		default:
			return nil, 0, fmt.Errorf("op %d: unknown op %q", i, r.Op)
		}
		op := porcupine.Operation{
			ClientId: clients[r.Client],
			Input:    kvInput{Op: r.Op, Key: r.Key, Value: r.Value},
			Call:     r.Call,
		}
		if r.Return == nil {
			pending++
			op.Return = last + 1
			op.Output = kvOutput{Unknown: true}
		} else {
			if r.Output == nil || *r.Return < r.Call {
				return nil, 0, fmt.Errorf("op %d: bad return", i)
			}
			op.Return = *r.Return
			op.Output = kvOutput{Value: r.Output.Value, Found: r.Output.Found}
		}
		ops = append(ops, op)
	}
	return ops, pending, nil
}

func main() {
	timeout := flag.Duration("timeout", 30*time.Second, "give up on one history after this long")
	vizDir := flag.String("viz", "", "write an HTML visualization of each failing history here")
	flag.Parse()
	if flag.NArg() == 0 {
		fmt.Fprintln(os.Stderr, "usage: lincheck [-timeout D] [-viz DIR] history.json...")
		os.Exit(2)
	}

	illegal, unknown := 0, 0
	totalOps, totalPending := 0, 0
	for _, path := range flag.Args() {
		data, err := os.ReadFile(path)
		if err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(2)
		}
		var h history
		if err := json.Unmarshal(data, &h); err != nil {
			fmt.Fprintf(os.Stderr, "%s: %v\n", path, err)
			os.Exit(2)
		}
		ops, pending, err := toOperations(h)
		if err != nil {
			fmt.Fprintf(os.Stderr, "%s: %v\n", path, err)
			os.Exit(2)
		}
		totalOps += len(ops)
		totalPending += pending

		res, info := porcupine.CheckOperationsVerbose(kvModel, ops, *timeout)
		switch res {
		case porcupine.Ok:
			continue
		case porcupine.Unknown:
			unknown++
			fmt.Printf("UNKNOWN  %s: timed out after %v (%d ops)\n", path, *timeout, len(ops))
			continue
		}
		illegal++
		msg := fmt.Sprintf("ILLEGAL  %s: seed %d, %s, %d ops", path, h.Seed, h.Variant, len(ops))
		if *vizDir != "" {
			out := filepath.Join(*vizDir, strings.TrimSuffix(filepath.Base(path), ".json")+".html")
			if err := porcupine.VisualizePath(kvModel, info, out); err == nil {
				msg += " -> " + out
			}
		}
		fmt.Println(msg)
	}
	fmt.Printf("lincheck: %d histories, %d ops (%d never returned): %d illegal, %d timed out\n",
		flag.NArg(), totalOps, totalPending, illegal, unknown)
	switch {
	case illegal > 0:
		os.Exit(1)
	case unknown > 0:
		os.Exit(2)
	}
}
