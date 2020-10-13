package main

import (
	"fmt"
	"log"
	"net/url"
	"time"

	"github.com/ovh/go-ovh/ovh"
)

const DataProcessingSubmit = "/cloud/project/%s/dataProcessing/jobs"
const DataProcessingLog = "/cloud/project/%s/dataProcessing/jobs/%s/logs"
const DataProcessingStatus = "/cloud/project/%s/dataProcessing/jobs/%s"

const JobStatusUNKNOWN = "UNKNOWN"
const JobStatusPENDING = "PENDING"
const JobStatusSUBMITTED = "SUBMITTED"
const JobStatusRUNNING = "RUNNING"
const JobStatusCANCELLING = "CANCELLING"
const JobStatusFAILED = "FAILED"
const JobStatusTERMINATED = "TERMINATED"
const JobStatusCOMPLETED = "COMPLETED"

const ParameterJobType = "job_type"
const ParameterMainClassName = "main_class_name"
const ParameterMainCode = "main_application_code"

const ParameterDriverCores = "driver_cores"
const ParameterDriverMemory = "driver_memory"
const ParameterDriverMemoryOverhead = "driver_memory_overhead"

const ParameterExecutorCores = "executor_cores"
const ParameterExecutorMemory = "executor_memory"
const ParameterExecutorMemoryOverhead = "executor_memory_overhead"
const ParameterExecutorNumber = "executor_num"

const ParameterPackages = "packages"
const ParameterRepositories = "repositories"

const ParameterArgs = "arguments"

const JobTypeJava = "java"
const JobTypePython = "python"

const Engine = "spark"

type (
	// JobStatus representation of JobStatus in OVH API
	JobStatus struct {
		ID               string                `json:"id"`
		Name             string                `json:"name"`
		Region           string                `json:"region"`
		Engine           string                `json:"engine"`
		ContainerName    string                `json:"containerName"`
		CreationDate     string                `json:"creationDate"`
		StartDate        string                `json:"startDate"`
		EndDate          string                `json:"endDate"`
		EngineVersion    string                `json:"engineVersion"`
		EngineParameters []*JobEngineParameter `json:"engineParameters"`
		Status           string                `json:"status"`
	}

	// JobEngineParameter representation of JobEngineParameter in OVH API
	JobEngineParameter struct {
		Name  string `json:"name"`
		Value string `json:"value"`
	}

	// JobLog representation of JobLog in OVH API
	JobLog struct {
		Logs        []*Log `json:"logs"`
		LogsAddress string `json:"logsAddress"`
		StartDate   string `json:"startDate"`
	}

	// Log representation of Log in OVH API
	Log struct {
		Content   string `json:"content"`
		ID        uint64 `json:"id"`
		Timestamp string `json:"timestamp"`
	}

	// JobSubmit representation of JobSubmit in OVH API
	JobSubmit struct {
		ContainerName    string                `json:"containerName"`
		Engine           string                `json:"engine"`
		Name             string                `json:"name"`
		Region           string                `json:"region"`
		EngineVersion    string                `json:"engineVersion"`
		EngineParameters []*JobEngineParameter `json:"engineParameters"`
	}

	Client struct {
		OVH          *ovh.Client
		lastPrintLog uint64
		JobID        string
	}
)

// GetStatus get status of the job from the API
func (c *Client) GetStatus(projectID string, jobID string) (*JobStatus, error) {

	job := &JobStatus{}
	path := fmt.Sprintf(DataProcessingStatus, url.QueryEscape(projectID), url.QueryEscape(jobID))
	return job, c.OVH.Get(path, job)
}

// GetLog get log of the job from the API
func (c *Client) GetLog(projectID string, jobID string, from string) (*JobLog, error) {
	jobLog := &JobLog{}
	path := fmt.Sprintf(DataProcessingLog, url.QueryEscape(projectID), url.QueryEscape(jobID))
	if from != "" {
		path = path + "?from=" + from
	}
	return jobLog, c.OVH.Get(path, jobLog)
}

// GetLog get log of the job from the API
func (c *Client) GetLogLast(projectID string, jobID string) (*JobLog, error) {
	t := time.Unix(0, int64(c.lastPrintLog)).In(time.UTC)
	from := t.Format("2006-01-02T15:04:05")
	return c.GetLog(projectID, jobID, from+".000")
}

// Submit job to the API
func (c *Client) Submit(projectID string, params *JobSubmit) (*JobStatus, error) {
	log.Printf("Submitting job %s ...", params.Name)
	job := &JobStatus{}

	path := fmt.Sprintf(DataProcessingSubmit, url.QueryEscape(projectID))
	return job, c.OVH.Post(path, params, job)
}

// Kill job
func (c *Client) Kill(projectID string, jobID string) error {
	path := fmt.Sprintf(DataProcessingStatus, url.QueryEscape(projectID), url.QueryEscape(jobID))
	return c.OVH.Delete(path, nil)
}
