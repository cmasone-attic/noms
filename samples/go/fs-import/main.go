// Copyright 2016 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

package main

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"sync"

	"github.com/attic-labs/noms/go/config"
	"github.com/attic-labs/noms/go/d"
	"github.com/attic-labs/noms/go/spec"
	"github.com/attic-labs/noms/go/types"
	"github.com/attic-labs/noms/go/util/profile"
	"github.com/attic-labs/noms/go/util/verbose"
	flag "github.com/juju/gnuflag"
)

type kv struct {
	k types.String
	v types.Ref
}

func main() {
	flag.Usage = func() {
		fmt.Fprintf(os.Stderr, "usage: %s <dir> <dataset>\n", os.Args[0])
		flag.PrintDefaults()
	}

	var concurrencyArg = flag.Int("concurrency", runtime.NumCPU(), "number of concurrent HTTP calls to retrieve remote resources")

	spec.RegisterDatabaseFlags(flag.CommandLine)
	verbose.RegisterVerboseFlags(flag.CommandLine)
	profile.RegisterProfileFlags(flag.CommandLine)

	flag.Parse(true)

	if len(flag.Args()) != 2 {
		d.CheckError(errors.New("expected dir and dataset args"))
	}

	dir := flag.Arg(0)
	if dir == "" {
		d.CheckErrorNoUsage(errors.New("Empty dir path"))
	}

	info, err := os.Stat(dir)
	if err != nil {
		d.CheckError(errors.New("couldn't stat dir"))
	}
	if !info.IsDir() {
		d.CheckError(errors.New(dir + " is not a directory"))
	}

	defer profile.MaybeStartProfile().Stop()

	cfg := config.NewResolver()
	db, ds, err := cfg.GetDataset(flag.Arg(1))
	if err != nil {
		fmt.Fprintf(os.Stderr, "Could not create dataset: %s\n", err)
		return
	}
	defer db.Close()

	done := make(chan struct{})
	defer close(done)
	paths, errc := walkFiles(done, dir)

	// Start a fixed number of goroutines to read and digest files.
	kvs := make(chan kv)
	var wg sync.WaitGroup
	wg.Add(*concurrencyArg)
	for i := 0; i < *concurrencyArg; i++ {
		go func() {
			fileImporter(db, done, paths, kvs)
			wg.Done()
		}()
	}
	go func() {
		wg.Wait()
		close(kvs)
	}()

	m := newStreamingMap(db, kvs)
	// Check whether the Walk failed.
	d.CheckErrorNoUsage(<-errc)

	_, err = db.CommitValue(ds, <-m)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error committing: %s\n", err)
		return
	}
}

func walkFiles(done <-chan struct{}, root string) (<-chan string, <-chan error) {
	paths := make(chan string)
	errc := make(chan error, 1)
	go func() {
		// Close the paths channel after Walk returns.
		defer close(paths)
		// No select needed for this send, since errc is buffered.
		errc <- filepath.Walk(root, func(path string, info os.FileInfo, err error) error {
			if err != nil {
				return err
			}
			if !info.Mode().IsRegular() {
				return nil
			}
			select {
			case paths <- path:
			case <-done:
				return errors.New("walk canceled")
			}
			return nil
		})
	}()
	return paths, errc
}

func fileImporter(vw types.ValueReadWriter, done <-chan struct{}, paths <-chan string, c chan<- kv) {
	for path := range paths {
		f, err := os.Open(path)
		d.Chk.NoError(err)
		defer f.Close()

		select {
		case c <- kv{types.String(path), vw.WriteValue(types.NewStreamingBlob(vw, f))}:
		case <-done:
			return
		}
	}
}

func newStreamingMap(vrw types.ValueReadWriter, kvs <-chan kv) <-chan types.Map {
	outChan := make(chan types.Map)
	go func() {
		gb := types.NewGraphBuilder(vrw, types.MapKind, false)
		for kv := range kvs {
			gb.MapSet(nil, kv.k, kv.v)
		}
		outChan <- gb.Build().(types.Map)
	}()
	return outChan
}
