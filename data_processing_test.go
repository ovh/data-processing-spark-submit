package main

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/ovh/go-ovh/ovh"
)

const (
	// In case you wonder, these are real *revoked* credentials
	MockApplicationKey    = "TDPKJdwZwAQPwKX2"
	MockApplicationSecret = "9ufkBmLaTQ9nz5yMUlg79taH0GNnzDjk"
	MockConsumerKey       = "5mBuy6SUQcRw2ZUxg0cG68BoDKpED4KY"

	MockTime = 1457018875

	ProjectID = "test"
	JobID     = "9b9c8d09-c95e-478b-a258-5f4dab826dad"
)

func initMockServer(InputRequest **http.Request, status int, responseBody string, requestBody *string, handlerSleep time.Duration) (*httptest.Server, *ovh.Client) {

	// Create a fake API server
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Save input parameters
		*InputRequest = r
		defer r.Body.Close()

		if r.RequestURI == "/auth/time" {
			w.WriteHeader(status)
			fmt.Fprint(w, MockTime)
			return
		}

		if requestBody != nil {
			reqBody, err := ioutil.ReadAll(r.Body)
			if err == nil {
				*requestBody = string(reqBody[:])
			}
		}

		if handlerSleep != 0 {
			time.Sleep(handlerSleep)
		}

		// Respond
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(status)
		fmt.Fprint(w, responseBody)
	}))

	// Create client
	client, _ := ovh.NewClient(ts.URL, MockApplicationKey, MockApplicationSecret, MockConsumerKey)

	return ts, client
}

func TestGetLog(t *testing.T) {

	jobLogStruct := &JobLog{
		Logs: []*Log{
			{
				Content:   "My first log",
				ID:        0,
				Timestamp: "2019-12-03T09:40:15Z",
			},
		},
		LogsAddress: "",
		StartDate:   "2019-12-03T09:40:13Z",
	}

	jobLog, _ := json.Marshal(jobLogStruct)
	// Init test
	var InputRequest *http.Request
	ts, ovh := initMockServer(&InputRequest, 200, string(jobLog), nil, time.Duration(0))
	defer ts.Close()

	client := &Client{
		OVH: ovh,
	}

	res, _ := client.GetLog(ProjectID, JobID, "")

	if res.StartDate != jobLogStruct.StartDate {
		t.Fail()
	}

	if res.Logs[0].Content != jobLogStruct.Logs[0].Content {
		t.Fail()
	}
}

func TestGetStatus(t *testing.T) {

	engineParameter := []*JobEngineParameter{
		{
			Name:  "arguments",
			Value: "1000, testargument",
		},
		{
			Name:  "driver_cores",
			Value: "2",
		},
		{
			Name:  "driver_memory",
			Value: "1",
		},
		{
			Name:  "driver_memory_overhead",
			Value: "512",
		},
		{
			Name:  "executor_cores",
			Value: "1",
		},
		{
			Name:  "executor_num",
			Value: "1",
		},
		{
			Name:  "executor_memory",
			Value: "2048",
		},
		{
			Name:  "executor_memory_overhead",
			Value: "512",
		},
		{
			Name:  "main_application_code",
			Value: "spark-examples.jar",
		},
		{
			Name:  "main_class_name",
			Value: "org.apache.spark.examples.SparkPi",
		},
		{
			Name:  "job_type",
			Value: "java",
		},
	}

	JobStatusStruct := &JobStatus{
		ID:               JobID,
		Name:             "hello",
		Region:           "GRA",
		Engine:           "spark",
		ContainerName:    "ovh-odp",
		CreationDate:     "2019-12-03T09:40:13Z",
		StartDate:        "2019-12-03T09:40:15Z",
		EndDate:          "",
		EngineVersion:    "2.4.3",
		EngineParameters: engineParameter,
		Status:           "RUNNING",
		ReturnCode:       '0',
	}

	jobStatus, _ := json.Marshal(JobStatusStruct)
	// Init test
	var InputRequest *http.Request
	ts, ovh := initMockServer(&InputRequest, 200, string(jobStatus), nil, time.Duration(0))
	defer ts.Close()

	client := &Client{
		OVH: ovh,
	}

	res, _ := client.GetStatus(ProjectID, JobID)

	if res.StartDate != JobStatusStruct.StartDate {
		t.Fail()
	}

	if res.Name != JobStatusStruct.Name {
		t.Fail()
	}

	if res.ReturnCode != JobStatusStruct.ReturnCode {
		t.Fail()
	}

}

func TestKill(t *testing.T) {

	// Init test
	var InputRequest *http.Request
	ts, ovh := initMockServer(&InputRequest, 200, "", nil, time.Duration(0))
	defer ts.Close()

	client := &Client{
		OVH: ovh,
	}

	err := client.Kill(ProjectID, JobID)

	if err != nil {
		t.Fail()
	}

}

func TestSubmit(t *testing.T) {

	engineParameter := []*JobEngineParameter{
		{
			Name:  "arguments",
			Value: "1000, testargument",
		},
		{
			Name:  "driver_cores",
			Value: "2",
		},
		{
			Name:  "driver_memory",
			Value: "1",
		},
		{
			Name:  "driver_memory_overhead",
			Value: "512",
		},
		{
			Name:  "executor_cores",
			Value: "1",
		},
		{
			Name:  "executor_num",
			Value: "1",
		},
		{
			Name:  "executor_memory",
			Value: "2048",
		},
		{
			Name:  "executor_memory_overhead",
			Value: "512",
		},
		{
			Name:  "main_application_code",
			Value: "spark-examples.jar",
		},
		{
			Name:  "main_class_name",
			Value: "org.apache.spark.examples.SparkPi",
		},
		{
			Name:  "job_type",
			Value: "java",
		},
	}

	JobStatusStruct := &JobStatus{
		ID:               JobID,
		Name:             "hello",
		Region:           "GRA",
		Engine:           "spark",
		ContainerName:    "ovh-odp",
		CreationDate:     "2019-12-03T09:40:13Z",
		StartDate:        "2019-12-03T09:40:15Z",
		EndDate:          "",
		EngineVersion:    "2.4.3",
		EngineParameters: engineParameter,
		Status:           "PENDING",
		ReturnCode:       '0',
	}

	jobStatus, _ := json.Marshal(JobStatusStruct)
	// Init test
	var InputRequest *http.Request
	ts, ovh := initMockServer(&InputRequest, 200, string(jobStatus), nil, time.Duration(0))
	defer ts.Close()

	client := &Client{
		OVH: ovh,
	}

	jobSubmit := &JobSubmit{
		ContainerName:    "ovh-odp",
		Engine:           "spark",
		Name:             "ovh-odp",
		Region:           "GRA",
		EngineVersion:    "2.4.3",
		EngineParameters: engineParameter,
	}

	res, _ := client.Submit(ProjectID, jobSubmit)

	if res.StartDate != JobStatusStruct.StartDate {
		t.Fail()
	}

	if res.Name != JobStatusStruct.Name {
		t.Fail()
	}

	if res.ReturnCode != JobStatusStruct.ReturnCode {
		t.Fail()
	}

}

func TestGetLastLog(t *testing.T) {
	jobLogStruct := &JobLog{
		Logs: []*Log{
			{
				Content:   "My first log",
				ID:        1,
				Timestamp: "2019-12-03T09:40:15Z",
			},
		},
		LogsAddress: "",
		StartDate:   "2019-12-03T09:40:13Z",
	}

	jobLog, _ := json.Marshal(jobLogStruct)
	// Init test
	var InputRequest *http.Request
	ts, ovh := initMockServer(&InputRequest, 200, string(jobLog), nil, time.Duration(0))
	defer ts.Close()

	client := &Client{
		OVH: ovh,
	}

	res, _ := client.GetLogLast(ProjectID, JobID)

	if res.StartDate != jobLogStruct.StartDate {
		t.Fail()
	}

	if res.Logs[0].Content != jobLogStruct.Logs[0].Content {
		t.Fail()
	}

}
