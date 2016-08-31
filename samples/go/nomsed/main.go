// Copyright 2016 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

package main

import (
	"fmt"
	"log"
	"os"

	"github.com/attic-labs/noms/go/dataset"
	"github.com/attic-labs/noms/go/spec"
	"github.com/attic-labs/noms/go/types"
	flag "github.com/juju/gnuflag"
)

func main() {
	var outDSStr = flag.String("out-ds-name", "", "output dataset to write to - if empty, defaults to input dataset")

	flag.Usage = func() {
		fmt.Fprintf(os.Stderr, "Given a dataset that's a Map<String, Struct>, nomsed updates all structs where <field> = <old-val> to <new-val>.\n\n")
		fmt.Fprintf(os.Stderr, "Usage: %s [-out-ds-name=<name>] <ds> <field> <old-val> <new-val>\n\n", os.Args[0])
		fmt.Fprintf(os.Stderr, "  <ds>      : Dataset to modify\n")
		fmt.Fprintf(os.Stderr, "  <field> : field within Struct to update\n")
		fmt.Fprintf(os.Stderr, "  <old-val> : Value in <field> to update\n")
		fmt.Fprintf(os.Stderr, "  <new-val> : new Value to replace <old-val>\n\n")
		fmt.Fprintf(os.Stderr, "Flags:\n\n")
		flag.PrintDefaults()
	}

	flag.Parse(false)

	if flag.NArg() == 0 {
		flag.Usage()
		return
	}

	if flag.NArg() != 4 {
		log.Fatalln("Incorrect number of arguments")
	}

	field := flag.Arg(1)
	if !types.IsValidStructFieldName(field) {
		log.Fatalln(field, "is not a valid Noms Struct field name.")
	}
	field = types.EscapeStructField(field)

	inDS, err := spec.GetDataset(flag.Arg(0))
	if err != nil {
		log.Fatalf("Invalid input dataset '%s': %s\n", flag.Arg(0), err)
	}

	inRoot, ok := inDS.MaybeHeadValue()
	if !ok {
		log.Fatalln("Input dataset has no data")
	}
	var valType *types.Type
	if valType, ok = getMapStructType(inRoot.Type()); !ok {
		log.Fatalln("Input dataset has wrong data type:", inRoot.Type().Describe())
	}
	if valType.Desc.(types.StructDesc).Field(field) == nil {
		log.Fatalf("Input dataset maps to Struct which lacks field %s. Actual type:\n%s\n", field, inRoot.Type().Describe())
	}

	oldVal, _, rem, err := types.ParsePathIndex(flag.Arg(2))
	if err != nil || rem != "" {
		log.Fatalf("Invalid new value: '%s': %s\n", flag.Arg(2), err)
	}

	newVal, _, rem, err := types.ParsePathIndex(flag.Arg(3))
	if err != nil || rem != "" {
		log.Fatalf("Invalid new value: '%s': %s\n", flag.Arg(3), err)
	}

	var outDS dataset.Dataset
	if *outDSStr == "" {
		outDS = inDS
	} else if dataset.DatasetRe.MatchString(*outDSStr) {
		outDS = dataset.NewDataset(inDS.Database(), *outDSStr)
	} else {
		log.Fatalf("Invalid output dataset name: %s\n", *outDSStr)
	}
	defer outDS.Database().Close()

	newMap := inRoot.(types.Map)
	newMap.IterAll(func(k, v types.Value) {
		s := v.(types.Struct)
		if s.Get(field).Equals(oldVal) {
			newMap = newMap.Set(k, s.Set(field, newVal))
		}
	})

	outDS.CommitValue(newMap)
}

func getMapStructType(t *types.Type) (st *types.Type, ok bool) {
	if t.Kind() == types.MapKind {
		desc := t.Desc.(types.CompoundDesc)
		if desc.ElemTypes[0].Kind() == types.StringKind && desc.ElemTypes[1].Kind() == types.StructKind {
			return desc.ElemTypes[1], true
		}
	}
	return
}
