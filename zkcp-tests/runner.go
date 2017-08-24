/*
 * Copyright (c) 2016, Salesforce.com, Inc.
 * All rights reserved.
 * Licensed under the BSD 3-Clause license.
 * For full license text, see LICENSE file or https://opensource.org/licenses/BSD-3-Clause
 */

package main

import (
	"fmt"
	"log"
	"os"
	"reflect"
	"strings"
	"text/tabwriter"
)

type Test struct {
	method   string
	failures int
	skipped  bool
}

func (t *Test) Fatal(msg interface{}) {
	t.failures++
	panic(msg)
}
func (t *Test) Fatalf(format string, args ...interface{}) {
	t.failures++
	panic(fmt.Sprintf(format, args...))
}
func (t *Test) Error(args ...interface{}) {
	t.failures++
	log.Println(args...)
}
func (t *Test) Errorf(format string, args ...interface{}) {
	t.failures++
	log.Printf(format, args...)
}
func (t *Test) SkipNow() {
	t.skipped = true
	panic(nil)
}

func quietlySkip(name string) bool {
	switch name {
	case "Fatal", "Fatalf", "Error", "Errorf", "SkipNow":
		return true
	}
	return false
}

func runTest(t *Test, method reflect.Method) {
	if t.method == "" {
		t.method = method.Name
	}
	defer func() {
		r := recover()
		if r != nil {
			log.Printf("recovered: %+v", r)
			t.failures++
		}
	}()
	log.Printf("Running %+v", method.Name)
	method.Func.Call([]reflect.Value{reflect.ValueOf(t)})
}

func testMethods() map[string]func(t *Test) {
	testType := reflect.ValueOf(&Test{}).Type()
	methods := make(map[string]func(t *Test), testType.NumMethod())
	for i := 0; i < testType.NumMethod(); i++ {
		method := testType.Method(i)
		if quietlySkip(method.Name) {
			continue
		}
		if !strings.HasPrefix(method.Name, "Test") {
			log.Printf("Skipping %v", method.Name)
			continue
		}
		if method.Type.String() != "func(*main.Test)" {
			log.Printf("Skipping %v with signature %+v", method.Name, method.Type.String())
			continue
		}
		methods[method.Name] = func(t *Test) { runTest(t, method) }
	}
	return methods
}

func main() {
	configs := []string{""}
	methods := testMethods()
	tests := make(map[string]map[string]*Test, len(configs))
	for _, config := range configs {
		tests[config] = make(map[string]*Test, len(methods))
		for method, run := range methods {
			t := &Test{}
			run(t)
			tests[config][method] = t
		}
	}
	w := tabwriter.NewWriter(os.Stdout, 0, 0, 1, ' ', 0)
	fmt.Fprintf(w, "\nSummary\t%v\n", strings.Join(configs, "\t"))
	for method, _ := range methods {
		outcomes := make([]string, 0, len(configs))
		for _, config := range configs {
			t, ok := tests[config][method]
			outcome := "ok"
			if !ok || t.skipped {
				outcome = "skipped"
			} else if t.failures > 0 {
				outcome = "failed"
			}
			outcomes = append(outcomes, outcome)
		}
		fmt.Fprintf(w, "%v\t%v\n", method, strings.Join(outcomes, "\t"))
	}
	w.Flush()
}
