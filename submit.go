package main

import (
	"fmt"
	"log"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"time"

	"data-processing-spark-submit/upload"
	"data-processing-spark-submit/utils"

	"github.com/Pallinder/go-randomdata"
	"github.com/alexflint/go-arg"
	"github.com/ovh/go-ovh/ovh"
	"gopkg.in/ini.v1"
)

const (
	LoopWaitSecond = 2
)

var (
	args struct {
		JobName                string   `arg:"env:JOB_NAME" help:"Job name (can be set with ENV vars JOB_NAME)"`
		Region                 string   `arg:"env:OS_REGION" default:"GRA" help:"Openstack region of the job (can be set with ENV vars OS_REGION)"`
		ProjectID              string   `arg:"env:OS_PROJECT_ID,required" help:"Openstack ProjectID (can be set with ENV vars OS_PROJECT_ID)"`
		Version                string   `arg:"env:SPARK_VERSION" default:"2.4.3" help:"Version of spark (can be set with ENV vars SPARK_VERSION)"`
		Upload                 string   `arg:"env:UPLOAD" help:"file path/dir to upload before running the job (can be set with ENV vars UPLOAD)"`
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

var (
	configPath = "configuration.ini"
)

type (
	OVHConf struct {
		Endpoint          string `ini:"endpoint"`
		ApplicationKey    string `ini:"application_key"`
		ApplicationSecret string `ini:"application_secret"`
		ConsumerKey       string `ini:"consumer_key"`
	}
)

// main ovh-spark-submit entry point
func main() {
	var err error

	jobSubmitValue := ParsArgs()

	conf, err := InitConf(configPath)
	if err != nil {
		log.Fatalf("Unable to load conf: %s", err)
	}

	ovhConf := new(OVHConf)
	if err := conf["ovh"].MapTo(ovhConf); err != nil {
		log.Fatalf("Unable to parse \"ovh\" conf: %s", err)
	}

	ovhClient, err := ovh.NewClient(
		ovhConf.Endpoint,
		ovhConf.ApplicationKey,
		ovhConf.ApplicationSecret,
		ovhConf.ConsumerKey,
	)
	if err != nil {
		log.Fatalf("Error while creating OVH Client: %s", err)
	}

	if args.Upload != "" {
		args.File = filepath.Clean(args.File)
		splitFile := strings.Split(args.File, "/")
		protocol := strings.TrimSuffix(splitFile[0], ":")
		storage, err := upload.New(conf[protocol], protocol)
		if err != nil {
			log.Fatalf("Error while Initialise Upload Storage: %s", err)
		}

		if storage == nil {
			log.Fatalf("No configuration found for protocol %s", protocol)
		}

		err = storage.Upload(args.Upload, strings.Join(splitFile[1:len(splitFile)-1], "/"))
		if err != nil {
			log.Fatalf("Error while uploading file(s): %s", err)
		}
	}

	wg := &sync.WaitGroup{}
	client := &Client{
		OVH: ovhClient,
	}

	job, err := client.Submit(args.ProjectID, jobSubmitValue)
	if err != nil {
		log.Fatalf("Unable to submit job: %s", err)
	}

	client.JobID = job.ID
	wg.Add(1)
	log.Printf("Job '%s' submitted with id %s", job.Name, job.ID)

	go func() {
		defer wg.Done()
		Loop(client, job)
	}()

	wg.Wait()

}

//initConf init configuration.ini file
func InitConf(confPath string) (map[string]*ini.Section, error) {
	cfg, err := ini.Load(confPath)
	if err != nil {
		return nil, err
	}
	conf := make(map[string]*ini.Section)
	for _, sectionName := range cfg.SectionStrings() {
		conf[sectionName], _ = cfg.GetSection(sectionName)
	}

	return conf, nil
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

//poll Status
func Loop(c *Client, job *JobStatus) {
	sigs := make(chan os.Signal, 2)
	signal.Notify(sigs, os.Interrupt, syscall.SIGTERM)
	var err error
statusLoop:
	for {
		select {
		case <-sigs:
			var s string
			fmt.Printf("Do you want to kill the Job (y/N): ")
			_, err := fmt.Scan(&s)
			if err != nil {
				log.Printf("Job not killed")
			}

			s = strings.TrimSpace(s)
			s = strings.ToLower(s)

			if s == "y" || s == "yes" {
				if err := c.Kill(args.ProjectID, c.JobID); err != nil {
					log.Printf("Job not killed: %d", err)
				}
				log.Printf("Job killed")
			} else {
				log.Printf("Job not killed")
			}
			return
		case <-time.After(LoopWaitSecond * time.Second):
			job, err = c.GetStatus(args.ProjectID, job.ID)
			if err != nil {
				log.Printf("Unable to retrieve status for job: %s", err)
			}
			switch job.Status {
			case JobStatusUNKNOWN, JobStatusSUBMITTED, JobStatusPENDING:
				log.Printf("Job is %s", job.Status)

			case JobStatusCANCELLING, JobStatusTERMINATED, JobStatusFAILED, JobStatusCOMPLETED:
				break statusLoop

			case JobStatusRUNNING:
				if jobLog, err := c.GetLogLast(args.ProjectID, job.ID); err == nil {
					c.lastPrintLog = PrintLog(jobLog.Logs)
				} else {
					log.Printf("Unable fetch job log: %s", err)
				}
			default:
				log.Printf("Status %s not implemeted yet", job.Status)
			}
		}
	}

	// print last logs
	retry := true
	for retry {
		if jobLog, err := c.GetLogLast(args.ProjectID, job.ID); err == nil {
			switch {
			case jobLog.LogsAddress != "":
				log.Printf("You can download your logs at %s", jobLog.LogsAddress)
				retry = false

			case len(jobLog.Logs) > 0:
				c.lastPrintLog = PrintLog(jobLog.Logs)
				time.Sleep(LoopWaitSecond * time.Second)
			default:
				retry = false
			}
		}

	}

}

// PrintLog Print Log and return last Print Log id
func PrintLog(jobLog []*Log) (lastPrintLog uint64) {
	for _, jLog := range jobLog {
		//don't print log already printed
		if lastPrintLog >= jLog.ID {
			continue
		}
		fmt.Println(jLog.Content)
		lastPrintLog = jLog.ID
	}
	return lastPrintLog
}
