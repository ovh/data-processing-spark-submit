package main

import (
	"os"
	"strings"
	"testing"
)

func TestParsArgsJava(t *testing.T) {
	// These are the args you would pass in on the command line
	os.Setenv("OS_PROJECT_ID", "1377b21260f05b410e4652445ac7c95b")
	os.Args = strings.Split("./ovh-spark-submit --class org.apache.spark.examples.SparkPi --driver-cores 1 --driver-memory 4G --executor-cores 1 --executor-memory 1G --num-executors 1 s3://odp/test/spark-examples.jar 1000", " ")

	job := ParsArgs()

	if job.ContainerName != "odp" {
		t.Fail()
	}

	if job.EngineVersion != "2.4.3" {
		t.Fail()
	}
	if job.Engine != "spark" {
		t.Fail()
	}

	if job.Region != "GRA" {
		t.Fail()
	}

	if args.ProjectID != "1377b21260f05b410e4652445ac7c95b" {
		t.Fail()
	}

	for _, params := range job.EngineParameters {
		if params.Name == ParameterDriverCores && params.Value != "1" {
			t.Fail()
		}

		if params.Name == ParameterDriverMemory && params.Value != "4096" {
			t.Fail()
		}

		if params.Name == ParameterDriverMemoryOverhead && params.Value != "409" {
			t.Fail()
		}

		if params.Name == ParameterExecutorMemory && params.Value != "1024" {
			t.Fail()
		}

		if params.Name == ParameterExecutorMemoryOverhead && params.Value != "384" {
			t.Fail()
		}

		if params.Name == ParameterMainCode && params.Value != "test/spark-examples.jar" {
			t.Fail()
		}

		if params.Name == ParameterJobType && params.Value != "java" {
			t.Fail()
		}
	}

}

func TestParsArgsPython(t *testing.T) {
	// These are the args you would pass in on the command line
	os.Setenv("OS_PROJECT_ID", "1377b21260f05b410e4652445ac7c95b")
	os.Args = strings.Split("./ovh-spark-submit --driver-cores 1 --driver-memory 4G --executor-cores 1 --executor-memory 1G --num-executors 1 s3://odp/test/spark-examples.py 1000", " ")

	job := ParsArgs()

	if job.ContainerName != "odp" {
		t.Fail()
	}

	if job.EngineVersion != "2.4.3" {
		t.Fail()
	}
	if job.Engine != "spark" {
		t.Fail()
	}

	if job.Region != "GRA" {
		t.Fail()
	}

	if args.ProjectID != "1377b21260f05b410e4652445ac7c95b" {
		t.Fail()
	}

	for _, params := range job.EngineParameters {
		if params.Name == ParameterDriverCores && params.Value != "1" {
			t.Fail()
		}

		if params.Name == ParameterDriverMemory && params.Value != "4096" {
			t.Fail()
		}

		if params.Name == ParameterDriverMemoryOverhead && params.Value != "409" {
			t.Fail()
		}

		if params.Name == ParameterExecutorMemory && params.Value != "1024" {
			t.Fail()
		}

		if params.Name == ParameterExecutorMemoryOverhead && params.Value != "384" {
			t.Fail()
		}

		if params.Name == ParameterMainCode && params.Value != "test/spark-examples.py" {
			t.Fail()
		}

		if params.Name == ParameterJobType && params.Value != "python" {
			t.Fail()
		}
	}

}

func TestParsArgs3(t *testing.T) {
	// These are the args you would pass in on the command line
	os.Setenv("OS_PROJECT_ID", "1377b21260f05b410e4652445ac7c95b")
	os.Args = strings.Split("./ovh-spark-submit --class org.apache.spark.examples.SparkPi --driver-cores 1 --driver-memory 4G --driver-memoryOverhead 385M --executor-cores 1 --executor-memory 1G --executor-memoryOverhead 385M --num-executors 1 s3://odp/test/spark-examples.jar 1000", " ")

	job := ParsArgs()

	if job.ContainerName != "odp" {
		t.Fail()
	}

	if job.EngineVersion != "2.4.3" {
		t.Fail()
	}
	if job.Engine != "spark" {
		t.Fail()
	}

	if job.Region != "GRA" {
		t.Fail()
	}

	if job.Ttl != "" {
		t.Fail()
	}

	if args.ProjectID != "1377b21260f05b410e4652445ac7c95b" {
		t.Fail()
	}

	for _, params := range job.EngineParameters {
		if params.Name == ParameterDriverCores && params.Value != "1" {
			t.Fail()
		}

		if params.Name == ParameterDriverMemory && params.Value != "4096" {
			t.Fail()
		}

		if params.Name == ParameterDriverMemoryOverhead && params.Value != "385" {
			t.Fail()
		}

		if params.Name == ParameterExecutorMemory && params.Value != "1024" {
			t.Fail()
		}

		if params.Name == ParameterExecutorMemoryOverhead && params.Value != "385" {
			t.Fail()
		}

		if params.Name == ParameterMainCode && params.Value != "test/spark-examples.jar" {
			t.Fail()
		}
	}

}

func TestInitConf(t *testing.T) {
	sec, err := InitConf("testdata/configuration.ini")
	if err != nil {
		t.Error()
	}

	if k, _ := sec["ovh"].GetKey("endpoint"); k.String() != "ovh-eu" {
		t.Fail()
	}
}

func TestInitConfErr(t *testing.T) {
	_, err := InitConf("testdata/fake.ini")
	if err == nil {
		t.Fail()
	}

}

func TestPrintLog(t *testing.T) {
	log := []*Log{
		{
			Content:   "My first log",
			ID:        1,
			Timestamp: "2019-12-03T09:40:15Z",
		},
		{
			Content:   "My seconf log",
			ID:        2,
			Timestamp: "2019-12-03T09:40:16Z",
		},
	}

	if PrintLog(log) != 2 {
		t.Fail()
	}
}
