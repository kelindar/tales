package main

import (
	"os"
	"path/filepath"
	"testing"
)

func TestLoadTexts(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "train.txt")
	if err := os.WriteFile(path, []byte("hello\n\nworld\n"), 0o644); err != nil {
		t.Fatal(err)
	}

	cwd, err := os.Getwd()
	if err != nil {
		t.Fatal(err)
	}
	if err := os.Chdir(dir); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.Chdir(cwd) })

	texts := loadTrainingTexts()
	if len(texts) != 2 || texts[0] != "hello" || texts[1] != "world" {
		t.Fatalf("unexpected texts: %#v", texts)
	}
}
