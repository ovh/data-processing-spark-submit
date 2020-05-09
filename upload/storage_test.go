package upload

import (
	"testing"

	"gopkg.in/ini.v1"
)

func TestNewErr(t *testing.T) {

	f := ini.Empty()
	sec, _ := f.NewRawSection("Fakeproto", `test=value`)
	_, err := New(sec, "test")
	if err == nil {
		t.Fail()
	}
}

func TestNewSwift(t *testing.T) {

	f := ini.Empty()
	sec, _ := f.NewRawSection("swift", `test=value`)
	_, err := New(sec, "swift")
	if err != nil {
		t.Fail()
	}
}
