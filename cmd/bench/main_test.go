package main

import "testing"

func TestMust(t *testing.T) {
	must(nil)

	defer func() {
		if recover() == nil {
			t.Fatal("expected panic")
		}
	}()
	must(errString("boom"))
}

type errString string

func (e errString) Error() string { return string(e) }
