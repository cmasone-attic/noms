// Copyright 2016 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

package main

import (
	"fmt"
	"log"
	"os"

	"github.com/attic-labs/noms/go/spec"
	"github.com/attic-labs/noms/go/types"
	flag "github.com/juju/gnuflag"
)

func main() {

	flag.Usage = func() {
		fmt.Fprintf(os.Stderr, "Given a dataset that's a Map<String, Struct>, nomsed updates all structs where <field> = <old-val> to <new-val>.\n\n")
		fmt.Fprintf(os.Stderr, "Usage: %s <ds>\n\n", os.Args[0])
		fmt.Fprintf(os.Stderr, "  <ds>      : Dataset to use\n")
	}

	flag.Parse(false)

	if flag.NArg() == 0 {
		flag.Usage()
		return
	}

	if flag.NArg() != 1 {
		log.Fatalln("Incorrect number of arguments")
	}

	inDS, err := spec.GetDataset(flag.Arg(0))
	if err != nil {
		log.Fatalf("Invalid input dataset '%s': %s\n", flag.Arg(0), err)
	}

	inRoot, ok := inDS.MaybeHeadValue()
	if !ok {
		log.Fatalln("Input dataset has no data")
	}

	if !checkMapSetType(inRoot.Type()) {
		log.Fatalln("Input dataset has wrong data type:", inRoot.Type().Describe())
	}

	width := 0
	total := uint64(0)
	samples := map[string]uint64{}
	inRoot.(types.Map).IterAll(func(k, v types.Value) {
		count := v.(types.Set).Len()
		total += count
		sample := string(k.(types.String))
		width = max(width, len(sample))
		samples[sample] += count
	})

	for k, count := range samples {
		fmt.Printf("%[1]*s: %d/%d\n", width, k, count, total)
	}
}

func checkMapSetType(t *types.Type) bool {
	if t.Kind() == types.MapKind {
		desc := t.Desc.(types.CompoundDesc)
		if desc.ElemTypes[0].Kind() == types.StringKind && desc.ElemTypes[1].Kind() == types.SetKind {
			return true
		}
	}
	return false
}

func max(a, b int) int {
	if a > b {
		return a
	}
	return b
}
