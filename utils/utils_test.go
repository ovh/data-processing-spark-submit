package utils

import (
	"os"
	"strings"
	"testing"
)

func TestDeductMemoryOverhead1(t *testing.T) {
	overhead := DeductMemoryOverhead("4G")

	if overhead != 409 {
		t.Fail()
	}
}

func TestDeductMemoryOverhead2(t *testing.T) {
	overhead := DeductMemoryOverhead("1G")

	if overhead != MinimalOverhead {
		t.Fail()
	}
}

func TestDeductMemoryOverhead3(t *testing.T) {
	overhead := DeductMemoryOverhead("y32")

	if overhead != MinimalOverhead {
		t.Fail()
	}
}

func TestParseSize1(t *testing.T) {
	size, err := ParseSize("4G")
	if err != nil {
		t.Error(err)
	}
	if size != 4096 {
		t.Fail()
	}
}

func TestParseSize2(t *testing.T) {
	size, err := ParseSize("1M")

	if err != nil {
		t.Error(err)
	}
	if size != 1 {
		t.Fail()
	}
}

func TestParseSize3(t *testing.T) {
	size, err := ParseSize("1T")

	if err != nil {
		t.Error(err)
	}
	if size != 1048576 {
		t.Fail()
	}
}

func TestParseSize4(t *testing.T) {
	size, err := ParseSize("1k")

	if err != nil {
		t.Error(err)
	}
	if size != 0 {
		t.Fail()
	}
}

func TestParseSize5(t *testing.T) {
	_, err := ParseSize("1")

	if err == nil {
		t.Fail()
	}

}

func TestCleanArgs(t *testing.T) {
	// These are the args you would pass in on the command line
	cmdLine := "./ovh-spark-submit --driver-cores 1 --driver-memory 4G --executor-cores 1 --executor-memory 1G --num-executors 1 s3://odp/test/spark-examples.jar 1000"
	os.Args = strings.Split(cmdLine, " ")

	CleanArgs()

	if strings.Join(os.Args, " ") != cmdLine {
		t.Fail()
	}

}

func TestCleanArgsWithSpace(t *testing.T) {
	// These are the args you would pass in on the command line
	cmdLine := "./ovh-spark-submit           --driver-cores 1"
	os.Args = strings.Split(cmdLine, " ")

	CleanArgs()

	if strings.Join(os.Args, " ") != "./ovh-spark-submit --driver-cores 1" {
		t.Fail()
	}

}
