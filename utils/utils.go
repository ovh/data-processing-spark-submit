package utils

import (
	"os"

	"github.com/dustin/go-humanize"
)

const MinimalOverhead = 384

// DeductMemoryOverhead Calculate Memory Overhead
// if calculated must > MinimalOverhead
func DeductMemoryOverhead(size string) (overhead uint64) {
	value, err := ParseSize(size)
	if err != nil {
		return MinimalOverhead
	}
	overhead = value / 10
	if overhead > MinimalOverhead {
		return
	}
	return MinimalOverhead

}

// ParseSize Parse Memory Size from string to MiB long
func ParseSize(size string) (uint64, error) {
	value, err := humanize.ParseBytes(size + "i")
	if err != nil {
		return 0, err
	}
	return value / 1024 / 1024, nil
}

// CleanArgs clean whitespace in os args
func CleanArgs() {
	var r []string
	for _, str := range os.Args {
		if str != "" {
			r = append(r, str)
		}
	}
	os.Args = r
}
