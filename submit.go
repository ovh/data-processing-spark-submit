package main

import (
	"errors"
	"fmt"
	"log"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	"data-processing-spark-submit/upload"
	"data-processing-spark-submit/utils"

	"github.com/Pallinder/go-randomdata"
	"github.com/alexflint/go-arg"
	"github.com/ovh/go-ovh/ovh"
	"github.com/peterhellberg/duration"
	"gopkg.in/ini.v1"
)

const (
	LoopWaitSecond = 2
	OVHConfig      = "ovh"
	SwiftConfig    = "swift"
)

var (
	args struct {
		JobName                string   `arg:"env:JOB_NAME" help:"Job name (can be set with ENV vars JOB_NAME)"`
		Region                 string   `arg:"env:OS_REGION" default:"GRA" help:"Openstack region of the job (can be set with ENV vars OS_REGION)"`
		ProjectID              string   `arg:"env:OS_PROJECT_ID,required" help:"Openstack ProjectID (can be set with ENV vars OS_PROJECT_ID)"`
		SparkVersion           string   `arg:"--spark-version,env:SPARK_VERSION" default:"2.4.3" help:"Version of spark (can be set with ENV vars SPARK_VERSION)"`
		Upload                 string   `arg:"env:UPLOAD" help:"Comma-delimited list of file path/dir to upload before running the job (can be set with ENV vars UPLOAD)"`
		Class                  string   `help:"main-class"`
		DriverCores            string   `arg:"--driver-cores,required"`
		DriverMemory           string   `arg:"--driver-memory,required" help:"Driver memory in (gigi/mebi)bytes (eg. \"10G\")"`
		DriverMemoryOverhead   string   `arg:"--driver-memoryOverhead" help:"Driver memoryOverhead in (gigi/mebi)bytes (eg. \"10G\")"`
		ExecutorCores          string   `arg:"--executor-cores,required"`
		ExecutorNum            string   `arg:"--num-executors,required"`
		ExecutorMemory         string   `arg:"--executor-memory,required" help:"Executor memory in (gigi/mebi)bytes (eg. \"10G\")"`
		ExecutorMemoryOverhead string   `arg:"--executor-memoryOverhead" help:"Executor memory in (gigi/mebi)bytes (eg. \"10G\")"`
		Packages               string   `arg:"--packages" help:"Comma-delimited list of Maven coordinates"`
		Repositories           string   `arg:"--repositories" help:"Comma-delimited list of additional repositories (or resolvers in SBT)"`
		PropertiesFile         string   `arg:"--properties-file" help:"Read properties from the given file"`
		TTL                    string   `arg:"--ttl" help:"Maximum \"Time To Live\" (in RFC3339 (duration) eg. \"P1DT30H4S\") of this job, after which it will be automatically terminated"`
		File                   string   `arg:"positional,required"`
		Parameters             []string `arg:"positional"`
	}
)

var (
	configPath         = "configuration.ini"
	SupportedProtocols = []string{SwiftConfig}
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

	protocols, err := validConfig(conf)
	if err != nil {
		log.Fatalf("Invalid conf: %s", err)
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
		containerName := splitFile[1]
		if inTheList(protocol, protocols) {
			storage, err := upload.New(conf[protocol], protocol)
			if err != nil {
				log.Fatalf("Error while initializing upload storage configurations: %s", err)
			}

			if storage == nil {
				log.Fatalf("No configuration found for protocol %s", protocol)
			}
			filesList := strings.Split(args.Upload, ",")
			for _, file := range filesList {
				err = storage.Upload(file, containerName)
				if err != nil {
					log.Fatalf("Error while uploading file(s): %s", err)
				}
			}
		} else {
			log.Fatalf("Error while initializing upload storage configurations: protocol %s isn't configured in %s or isn't supported", protocol, configPath)
		}
	}

	client := &Client{
		OVH: ovhClient,
	}

	job, err := client.Submit(args.ProjectID, jobSubmitValue)
	if err != nil {
		if ovherr, ok := err.(*ovh.APIError); ok {
			if err.Error() == "Error 422: \"Unprocessable Entity\"" {
				log.Fatalf("Unable to submit job: %s :: %s :: %v. "+
					"Please check that your requested job complies with the OVHcloud Data Processing capabilities "+
					"(https://docs.ovh.com/gb/en/data-processing/capabilities/#the-apache-spark-job-in-data-processing-is-limited-to)", err, ovherr.Class, GetErrorDetails(ovherr))
			} else {
				log.Fatalf("Unable to submit job: %s :: %s :: %v.", err, ovherr.Class, GetErrorDetails(ovherr))
			}
		}

		log.Fatalf("Unable to submit job: %s", err)
	}

	client.JobID = job.ID
	log.Printf("Job '%s' submitted with id %s", job.Name, job.ID)

	returnCodeChan := make(chan int)
	defer close(returnCodeChan)

	go func() {
		job := Loop(client, job)
		log.Printf("Job status is : %s", job.Status)
		if job.Status == "COMPLETED" {
			log.Printf("Job exit code : %v", job.ReturnCode)
		}
		returnCodeChan <- int(job.ReturnCode)
	}()
	os.Exit(<-returnCodeChan)
}

// initConf init configuration.ini file
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

// ParsArgs Parse args and return a JobSubmit
func ParsArgs() *JobSubmit {
	// clean args
	utils.CleanArgs()
	p := arg.MustParse(&args)

	jobSubmit := &JobSubmit{
		Engine:           Engine,
		Name:             args.JobName,
		Region:           args.Region,
		EngineVersion:    args.SparkVersion,
		EngineParameters: []*JobEngineParameter{},
		TTL:              args.TTL,
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

	if args.Packages != "" {
		jobSubmit.EngineParameters = append(jobSubmit.EngineParameters, &JobEngineParameter{
			Name:  ParameterPackages,
			Value: args.Packages,
		})
	}

	if args.Repositories != "" {
		jobSubmit.EngineParameters = append(jobSubmit.EngineParameters, &JobEngineParameter{
			Name:  ParameterRepositories,
			Value: args.Repositories,
		})
	}

	if args.TTL != "" {
		_, err := duration.Parse(args.TTL)
		if err != nil {
			p.Fail("Invalid value for --ttl. It must be in RFC3339 (duration) format (i.e. PT30H for 30 hours)")
		}
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

	if args.PropertiesFile != "" {
		jobSubmit.EngineParameters = append(jobSubmit.EngineParameters, &JobEngineParameter{
			Name:  ParameterPropertiesFile,
			Value: args.PropertiesFile,
		})
	}

	return jobSubmit
}

// poll Status
func Loop(c *Client, job *JobStatus) *JobStatus {
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
			return job
		case <-time.After(LoopWaitSecond * time.Second):
			job, err = c.GetStatus(args.ProjectID, job.ID)
			if err != nil {
				log.Printf("Unable to retrieve status for job: %s", err)
				break
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
	return job
}

// PrintLog Print Log and return last Print Log id
func PrintLog(jobLog []*Log) (lastPrintLog int64) {
	for _, jLog := range jobLog {
		// don't print log already printed
		if lastPrintLog >= jLog.ID {
			continue
		}
		fmt.Println(jLog.Content)
		lastPrintLog = jLog.ID
	}
	return lastPrintLog
}

// test if a value is in the given list
func inTheList(value string, values []string) bool {
	for _, element := range values {
		if value == element {
			return true
		}
	}
	return false
}

// test if the given configurations are valid and list the protocols configured
func validConfig(configSections map[string]*ini.Section) ([]string, error) {
	confList := make([]string, 0, len(configSections))
	var protocolsList []string
	for name := range configSections {
		confList = append(confList, name)
		if inTheList(name, SupportedProtocols) {
			protocolsList = append(protocolsList, name)
		}
	}

	if !inTheList(OVHConfig, confList) {
		return protocolsList, errors.New("missing [ovh] configurations in " + configPath)
	}

	return protocolsList, nil
}
