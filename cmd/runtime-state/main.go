// runtime-state operates on stopped collectors' local recovery files. Database
// conversion and domain replay remain the responsibility of each collector.
package main

import (
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"os"

	"github.com/vphoenix/crypto-market-info/internal/runtimecheckpoint"
)

func run(args []string) error {
	if len(args) == 0 {
		return errors.New("usage: runtime-state verify|export|recover-previous|epoch-init|epoch-rotate|epoch-show [flags]")
	}
	fs := flag.NewFlagSet(args[0], flag.ContinueOnError)
	dir := fs.String("state-dir", "", "stopped collector runtime directory")
	bindingPath := fs.String("binding", "", "expected binding JSON (from verified conversion)")
	destination := fs.String("destination", "", "new export/recovery directory; must not exist")
	epochPath := fs.String("epoch-file", "", "database recovery epoch file, outside database backups")
	target := fs.String("target", "", "exact database target identity")
	expected := fs.String("expected-epoch", "", "current epoch required for compare-and-swap rotation")
	reason := fs.String("reason", "", "maintenance reason")
	if err := fs.Parse(args[1:]); err != nil {
		return err
	}
	if fs.NArg() != 0 {
		return errors.New("unexpected arguments")
	}
	printJSON := func(v any) error { enc := json.NewEncoder(os.Stdout); enc.SetIndent("", "  "); return enc.Encode(v) }
	switch args[0] {
	case "epoch-init", "epoch-rotate", "epoch-show":
		if *epochPath == "" || *target == "" {
			return errors.New("epoch-file and target required")
		}
		if args[0] == "epoch-show" {
			e, err := runtimecheckpoint.ReadEpoch(*epochPath, *target)
			if err != nil {
				return err
			}
			return printJSON(e)
		}
		if args[0] == "epoch-rotate" && *expected == "" {
			return errors.New("epoch-rotate requires expected-epoch")
		}
		if args[0] == "epoch-init" && *expected != "" {
			return errors.New("epoch-init cannot replace an existing epoch")
		}
		e, err := runtimecheckpoint.RotateEpoch(*epochPath, *target, *expected, *reason)
		if err != nil {
			return err
		}
		return printJSON(e)
	case "verify", "export", "recover-previous":
		if *dir == "" || *bindingPath == "" {
			return errors.New("state-dir and binding required")
		}
		f, err := os.Open(*bindingPath)
		if err != nil {
			return err
		}
		defer f.Close()
		var b runtimecheckpoint.Binding
		d := json.NewDecoder(f)
		d.DisallowUnknownFields()
		if err = d.Decode(&b); err != nil {
			return err
		}
		if args[0] == "recover-previous" {
			if *destination == "" {
				return errors.New("destination required")
			}
			if err = runtimecheckpoint.RecoverPrevious(*dir, *destination, b, 0); err != nil {
				return err
			}
			return printJSON(map[string]string{"recovered_directory": *destination, "status": "domain commit verification and replay required before collection"})
		}
		if args[0] == "export" && *destination == "" {
			return errors.New("destination required")
		}
		exportDir := ""
		if args[0] == "export" {
			exportDir = *destination
		}
		v, err := runtimecheckpoint.InspectDirectory(*dir, b, 0, exportDir)
		if err != nil {
			return err
		}
		return printJSON(v)
	default:
		return fmt.Errorf("unknown operation %q", args[0])
	}
}
func main() {
	if err := run(os.Args[1:]); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
