package main

import (
	"encoding/json"
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

	randomdata "github.com/Pallinder/go-randomdata"
	arg "github.com/alexflint/go-arg"
	"github.com/hjson/hjson-go/v4"
	"github.com/imdario/mergo"
	"github.com/ovh/go-ovh/ovh"
	"github.com/peterhellberg/duration"
	ini "gopkg.in/ini.v1"
)

const (
	LoopWaitSecond = 2
	OVHConfig      = "ovh"
	SwiftConfig    = "swift"
)

var (
	args     CLIArgs
	fileArgs CLIArgs
)

var (
	defaultConfigPath  = "configuration.ini"
	SupportedProtocols = []string{SwiftConfig}
)

type (
	OVHConf struct {
		Endpoint          string `ini:"endpoint"`
		ApplicationKey    string `ini:"application_key"`
		ApplicationSecret string `ini:"application_secret"`
		ConsumerKey       string `ini:"consumer_key"`
	}

	CLIArgs struct {
		JobName                string   `json:"jobname" ini:"jobname" arg:"env:JOB_NAME" help:"Job name (can be set with ENV vars JOB_NAME)"`
		Region                 string   `json:"region" ini:"region" arg:"env:OS_REGION" default:"GRA" help:"Openstack region of the job (can be set with ENV vars OS_REGION)"`
		ProjectID              string   `json:"projectid" ini:"projectid" arg:"env:OS_PROJECT_ID" help:"Openstack ProjectID (can be set with ENV vars OS_PROJECT_ID)"`
		SparkVersion           string   `json:"spark-version" ini:"spark-version" arg:"--spark-version,env:SPARK_VERSION" default:"2.4.3" help:"Version of spark (can be set with ENV vars SPARK_VERSION)"`
		Upload                 string   `json:"upload" ini:"upload" arg:"env:UPLOAD" help:"Comma-delimited list of file path/dir to upload before running the job (can be set with ENV vars UPLOAD)"`
		Class                  string   `json:"class" ini:"class" help:"main-class"`
		DriverCores            string   `json:"driver-cores" ini:"driver-cores" arg:"--driver-cores"`
		DriverMemory           string   `json:"driver-memory" ini:"driver-memory" arg:"--driver-memory" help:"Driver memory in (gigi/mebi)bytes (eg. \"10G\")"`
		DriverMemoryOverhead   string   `json:"driver-memoryOverhead" ini:"driver-memoryOverhead" arg:"--driver-memoryOverhead" help:"Driver memoryOverhead in (gigi/mebi)bytes (eg. \"10G\")"`
		ExecutorCores          string   `json:"executor-cores" ini:"executor-cores" arg:"--executor-cores"`
		ExecutorNum            string   `json:"num-executors" ini:"num-executors" arg:"--num-executors"`
		ExecutorMemory         string   `json:"executor-memory" ini:"executor-memory" arg:"--executor-memory" help:"Executor memory in (gigi/mebi)bytes (eg. \"10G\")"`
		ExecutorMemoryOverhead string   `json:"executor-memoryOverhead" ini:"executor-memoryOverhead" arg:"--executor-memoryOverhead" help:"Executor memory in (gigi/mebi)bytes (eg. \"10G\")"`
		Packages               string   `json:"packages" ini:"packages" arg:"--packages" help:"Comma-delimited list of Maven coordinates"`
		Repositories           string   `json:"repositories" ini:"repositories" arg:"--repositories" help:"Comma-delimited list of additional repositories (or resolvers in SBT)"`
		PropertiesFile         string   `json:"properties-file" ini:"properties-file" arg:"--properties-file" help:"Read properties from the given file"`
		TTL                    string   `json:"ttl" ini:"ttl" arg:"--ttl" help:"Maximum \"Time To Live\" (in RFC3339 (duration) eg. \"P1DT30H4S\") of this job, after which it will be automatically terminated"`
		ParametersIni          string   `json:"parameters" arg:"-" ini:"parameters"`
		Config                 *string  `arg:"--conf"`
		JobConfig              *string  `arg:"--job-conf"`
		File                   string   `json:"file" ini:"file" arg:"positional"`
		Parameters             []string `arg:"positional"`
		InterruptedJobExitCode int64    `arg:"--interrupted-exit-code" help:"Exit code for interrupted job (finised with TERMINATED or FAILED status), default is 0"`
	}
)

// main ovh-spark-submit entry point
func main() {
	var err error

	// clean args and parse them to see if we need to process files or not
	utils.CleanArgs()
	parser := arg.MustParse(&args)

	if args.Config == nil {
		args.Config = &defaultConfigPath
	}

	conf, err := InitConf(*args.Config)
	if err != nil {
		log.Fatalf("Unable to load conf: %s", err)
	}
	if _, ok := conf["spark"]; ok {
		err = conf["spark"].MapTo(&args)

		if err != nil {
			log.Fatalf("Unable to load conf: %s", err)
		}
	}

	if args.JobConfig != nil {
		if _, err := os.Stat(*args.JobConfig); err == nil {
			content, err := os.ReadFile(*args.JobConfig)
			if err != nil {
				log.Fatalf("Unable to load job conf: %s", err)
			}
			switch {
			case strings.HasSuffix(*args.JobConfig, ".json"):
				if err := json.Unmarshal(content, &fileArgs); err != nil {
					log.Fatalf("Unable to load job conf: %s", err)
				}
			case strings.HasSuffix(*args.JobConfig, ".hjson"):
				if err := hjson.Unmarshal(content, &fileArgs); err != nil {
					log.Fatalf("Unable to load job conf: %s", err)
				}
			default:
				log.Fatalf("Job configuration must be a json or hjson file and is currently: %s", *args.JobConfig)
			}

		} else if !strings.HasSuffix(*args.JobConfig, ".json") && !strings.HasSuffix(*args.JobConfig, ".hjson") {
			log.Fatalf("Job configuration must be a json or hjson file and is currently: %s", *args.JobConfig)
		}
	}

	jobSubmitValue := ParsArgs(*parser)

	protocols, err := validConfig(conf, *args.Config)
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
			log.Fatalf("Error while initializing upload storage configurations: protocol %s isn't configured in %s or isn't supported", protocol, *args.Config)
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

	go func() {
		job := Loop(client, job)
		returnCodeChan <- getExitCode(job, args.InterruptedJobExitCode)
	}()

	// return the channel to a value, and get the defer close channel
	returnedExitCode := <-returnCodeChan
	close(returnCodeChan)
	os.Exit(returnedExitCode)
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
func ParsArgs(p arg.Parser) *JobSubmit {
	if args.ParametersIni != "" {
		args.Parameters = strings.Split(args.ParametersIni, ",")
	}

	err := mergo.Merge(&args, fileArgs)

	if err != nil {
		log.Fatalf("Error while initializing configurations: %s", err)
	}

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

	if args.ProjectID == "" {
		p.Fail("--projectid is required")
	}
	if args.DriverCores == "" {
		p.Fail("--driver-cores is required")
	}
	if args.DriverMemory == "" {
		p.Fail("--driver-memory is required")
	}
	if args.ExecutorMemory == "" {
		p.Fail("--executor-memory is required")
	}
	if args.ExecutorNum == "" {
		p.Fail("--num-executors is required")
	}
	if args.ExecutorCores == "" {
		p.Fail("--executor-cores is required")
	}
	if args.File == "" {
		p.Fail("file is required")
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
func PrintLog(jobLog []*Log) (lastPrintLog uint64) {
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

// Process the exit code depending on the job status
func getExitCode(job *JobStatus, interruptedJobExitCode int64) int {
	log.Printf("Job status is : %s", job.Status)
	codeToReturn := job.ReturnCode

	switch job.Status {
	case JobStatusCOMPLETED:
		log.Printf("Job finished with code : %v", job.ReturnCode)
	case JobStatusTERMINATED, JobStatusFAILED:
		log.Printf("Job has been interrupted, spark-submit exiting with code : %v", interruptedJobExitCode)
		codeToReturn = interruptedJobExitCode
	default:
		log.Printf("Status %s not implemeted yet", job.Status)
	}
	return int(codeToReturn)
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
func validConfig(configSections map[string]*ini.Section, configPath string) ([]string, error) {
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
