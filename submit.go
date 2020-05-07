package main

import (
	"fmt"
	"log"
	"net/url"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	"github.com/Pallinder/go-randomdata"
	"github.com/alexflint/go-arg"
	"github.com/ovh/go-ovh/ovh"

	"data-processing-spark-submit/utils"
)

const (
	LoopWaitSecond = 5
)

var (
	args struct {
		JobName                string   `arg:"env:JOB_NAME" help:"Job name (can be set with ENV VARS JOB_NAME)"`
		Region                 string   `arg:"env:OS_REGION" default:"GRA" help:"Openstack region of the job (can be set with ENV VARS OS_REGION)"`
		ProjectID              string   `arg:"env:OS_PROJECT_ID,required" help:"Openstack ProjectID (can be set with ENV VARS OS_PROJECT_ID)"`
		Version                string   `arg:"env:SPARK_VERSION" default:"2.4.3" help:"Version of spark (can be set with ENV VARS SPARK_VERSION)"`
		Class                  string   `help:"main-class"`
		DriverCores            string   `arg:"--driver-cores,required"`
		DriverMemory           string   `arg:"--driver-memory,required" help:"Driver memory in (gigi/mebi)bytes (eg. \"10G\")"`
		DriverMemoryOverhead   string   `arg:"--driver-memoryOverhead" help:"Driver memoryOverhead in (gigi/mebi)bytes (eg. \"10G\")"`
		ExecutorCores          string   `arg:"--executor-cores,required"`
		ExecutorNum            string   `arg:"--num-executors,required"`
		ExecutorMemory         string   `arg:"--executor-memory,required" help:"Executor memory in (gigi/mebi)bytes (eg. \"10G\")"`
		ExecutorMemoryOverhead string   `arg:"--executor-memoryOverhead" help:"Executor memory in (gigi/mebi)bytes (eg. \"10G\")"`
		File                   string   `arg:"positional,required"`
		Parameters             []string `arg:"positional"`
	}
)

type (
	Client struct {
		OVH          *ovh.Client
		lastPrintLog uint64
		JobID        string
	}
)

// main ovh-spark-submit entry point
func main() {

	jobSubmitValue := ParsArgs()

	var err error
	ovh, err := ovh.NewDefaultClient()
	if err != nil {
		log.Fatalf("Error while creating OVH Client: %s\nYou need to create an application; please visite this page https://api.ovh.com/createApp/ and create your ovh.conf file\n\t[default]\n\t; general configuration: default endpoint\n\tendpoint=ovh-eu\n\n\t[ovh-eu]\n\t; configuration specific to 'ovh-eu' endpoint\n\tapplication_key=my_app_key", err)
	}
	client := &Client{
		OVH: ovh,
	}

	job := client.Submit(args.ProjectID, jobSubmitValue)
	SetupCloseHandler(client)
	//poll Status
statusLoop:
	for {
		switch job.Status {
		case JobStatusUNKNOWN, JobStatusSUBMITTED, JobStatusPENDING:
			log.Printf("Job is %s", job.Status)

		case JobStatusCANCELLING, JobStatusTERMINATED, JobStatusFAILED, JobStatusCOMPLETED:
			break statusLoop

		case JobStatusRUNNING:
			client.PrintLog(args.ProjectID, job.ID)
		}

		time.Sleep(time.Second * LoopWaitSecond)
		job = client.GetStatus(args.ProjectID, job.ID)
	}

	//print last log
	client.PrintLog(args.ProjectID, job.ID)
	log.Printf("Job is %s", job.Status)
	logs := client.GetLog(args.ProjectID, job.ID, "")
	if logs.LogsAddress != "" {
		log.Printf("You can download your logs at %s", logs.LogsAddress)
	}
}

// SetupCloseHandler creates a 'listener' on a new goroutine which will notify the
// program if it receives an interrupt from the OS. We then handle this by calling
// our clean up procedure and exiting the program.
func SetupCloseHandler(client *Client) {
	c := make(chan os.Signal)
	signal.Notify(c, os.Interrupt, syscall.SIGTERM)
	go func() {
		<-c
		client.Kill(args.ProjectID, client.JobID)
		log.Printf("Job is killed")
		os.Exit(0)
	}()
}

//ParsArgs Parse args and return a JobSubmit
func ParsArgs() *JobSubmit {
	//clean args
	utils.CleanArgs()
	p := arg.MustParse(&args)
	jobSubmit := &JobSubmit{
		Engine:           Engine,
		Name:             args.JobName,
		Region:           args.Region,
		EngineVersion:    args.Version,
		EngineParameters: []*JobEngineParameter{},
	}

	if args.JobName == "" {
		jobSubmit.Name = randomdata.SillyName()
	}
	if strings.EqualFold(filepath.Ext(args.File), ".jar") {
		if args.Class == "" {
			p.Fail("You must provide --class when using jar file")
		}
		jobSubmit.EngineParameters = append(jobSubmit.EngineParameters, &JobEngineParameter{
			Name:  ParameterJobType,
			Value: JobTypeJava,
		})

		jobSubmit.EngineParameters = append(jobSubmit.EngineParameters, &JobEngineParameter{
			Name:  ParameterMainClassName,
			Value: args.Class,
		})
	} else {
		jobSubmit.EngineParameters = append(jobSubmit.EngineParameters, &JobEngineParameter{
			Name:  ParameterJobType,
			Value: JobTypePython,
		})
	}
	args.File = filepath.Clean(args.File)
	splitFile := strings.Split(args.File, "/")
	jobSubmit.ContainerName = splitFile[1]

	jobSubmit.EngineParameters = append(jobSubmit.EngineParameters, &JobEngineParameter{
		Name:  ParameterMainCode,
		Value: strings.Join(splitFile[2:], "/"),
	})

	value, err := utils.ParseSize(args.DriverMemory)
	if err != nil {
		p.Fail("Invalid value for --driver-memory")
	}

	jobSubmit.EngineParameters = append(jobSubmit.EngineParameters, &JobEngineParameter{
		Name:  ParameterDriverMemory,
		Value: fmt.Sprintf("%d", value),
	})

	if args.DriverMemoryOverhead != "" {
		value, err = utils.ParseSize(args.DriverMemoryOverhead)
		if err != nil {
			p.Fail("Invalid value for --driver-memoryOverhead")
		}
		jobSubmit.EngineParameters = append(jobSubmit.EngineParameters, &JobEngineParameter{
			Name:  ParameterDriverMemoryOverhead,
			Value: fmt.Sprintf("%d", value),
		})
	} else {
		jobSubmit.EngineParameters = append(jobSubmit.EngineParameters, &JobEngineParameter{
			Name:  ParameterDriverMemoryOverhead,
			Value: fmt.Sprintf("%d", utils.DeductMemoryOverhead(args.DriverMemory)),
		})
	}

	value, err = utils.ParseSize(args.ExecutorMemory)
	if err != nil {
		p.Fail("Invalid value for --executor-memory")
	}
	jobSubmit.EngineParameters = append(jobSubmit.EngineParameters, &JobEngineParameter{
		Name:  ParameterExecutorMemory,
		Value: fmt.Sprintf("%d", value),
	})

	if args.ExecutorMemoryOverhead != "" {
		value, err = utils.ParseSize(args.ExecutorMemoryOverhead)
		if err != nil {
			p.Fail("Invalid value for --executor-memoryOverhead")
		}
		jobSubmit.EngineParameters = append(jobSubmit.EngineParameters, &JobEngineParameter{
			Name:  ParameterExecutorMemoryOverhead,
			Value: fmt.Sprintf("%d", value),
		})
	} else {
		jobSubmit.EngineParameters = append(jobSubmit.EngineParameters, &JobEngineParameter{
			Name:  ParameterExecutorMemoryOverhead,
			Value: fmt.Sprintf("%d", utils.DeductMemoryOverhead(args.ExecutorMemory)),
		})
	}

	jobSubmit.EngineParameters = append(jobSubmit.EngineParameters, &JobEngineParameter{
		Name:  ParameterExecutorNumber,
		Value: args.ExecutorNum,
	})

	jobSubmit.EngineParameters = append(jobSubmit.EngineParameters, &JobEngineParameter{
		Name:  ParameterExecutorCores,
		Value: args.ExecutorCores,
	})

	jobSubmit.EngineParameters = append(jobSubmit.EngineParameters, &JobEngineParameter{
		Name:  ParameterDriverCores,
		Value: args.DriverCores,
	})

	jobSubmit.EngineParameters = append(jobSubmit.EngineParameters, &JobEngineParameter{
		Name:  ParameterArgs,
		Value: strings.Join(args.Parameters, ", "),
	})

	return jobSubmit
}

// GetStatus get status of the job from the API
func (c *Client) GetStatus(projectID string, jobID string) *JobStatus {

	job := &JobStatus{}
	path := fmt.Sprintf(DataProcessingStatus, url.QueryEscape(projectID), url.QueryEscape(jobID))
	if err := c.OVH.Get(path, job); err != nil {
		log.Fatalf("Unable to retrieve status for job: %s", err)
	}
	return job
}

// GetLog get log of the job from the API
func (c *Client) GetLog(projectID string, jobID string, from string) *JobLog {
	jobLog := &JobLog{}
	path := fmt.Sprintf(DataProcessingLog, url.QueryEscape(projectID), url.QueryEscape(jobID))
	if from != "" {
		path = path + "?from=" + from
	}

	if err := c.OVH.Get(path, jobLog); err != nil {
		log.Printf("Unable get job log: %s", err)
	}

	return jobLog
}

// PrintLog Print log from the API
func (c *Client) PrintLog(projectID string, jobID string) {

	t := time.Unix(0, int64(c.lastPrintLog)).In(time.UTC)
	from := t.Format("2006-01-02T15:04:05")
	jobLog := c.GetLog(projectID, jobID, from+".000")

	for _, jLog := range jobLog.Logs {
		//don't print log already printed
		if c.lastPrintLog >= jLog.ID {
			continue
		}
		fmt.Println(jLog.Content)
		c.lastPrintLog = jLog.ID
	}
}

// Submit job to the API
func (c *Client) Submit(projectID string, params *JobSubmit) *JobStatus {
	log.Printf("Submitting job %s ...", params.Name)
	job := &JobStatus{}

	path := fmt.Sprintf(DataProcessingSubmit, url.QueryEscape(projectID))
	if err := c.OVH.Post(path, params, job); err != nil {
		log.Fatalf("Unable to submit job: %s", err)
	}
	c.JobID = job.ID
	log.Printf("Job '%s' submitted with id %s", job.Name, job.ID)
	return job
}

// Kill job
func (c *Client) Kill(projectID string, jobID string) {

	path := fmt.Sprintf(DataProcessingStatus, url.QueryEscape(projectID), url.QueryEscape(jobID))
	if err := c.OVH.Delete(path, nil); err != nil {
		log.Fatalf("Unable to kill job: %s", err)
	}
}
