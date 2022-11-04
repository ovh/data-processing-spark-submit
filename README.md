# OVHcloud Data Processing Spark-submit

[![Gitter](https://img.shields.io/gitter/room/nwjs/nw.js.svg)](https://gitter.im/ovh/data-processing)
[![travis](https://travis-ci.org/ovh/data-processing-spark-submit.svg?branch=master)](https://travis-ci.org/ovh/data-processing-spark-submit)

With OVHcloud Data Processing, you can run your processing job over the cloud, in a fast, easy and cost-efficient way. 
Everything is managed and secured by OVHcloud and you just need to define how much resources you would like. 
You can learn more about OVHcloud Data Processing [here](https://docs.ovh.com/gb/en/data-processing/overview/).

In this repository you will find a spark-submit wrapper to run spark job on OVHcloud Data Processing. 

## Configuration

Create an OVHcloud token by visiting  https://eu.api.ovh.com/createToken/
and add right GET/POST/PUT on endpoint /cloud/project/\*/dataProcessing/\*

If you want to use the auto upload you need set storage's parameters too.

Supported storage protocol :
 - swift (OVHcloud Object Storage with Keystone v3 authentication)

Then create the configuration file ``configuration.ini`` in the same directory as below :

```ini
[ovh]
; configuration specific to 'ovh-eu' endpoint
endpoint=ovh-eu
application_key=my_app_key
application_secret=my_application_secret
consumer_key=my_consumer_key

; configuration specific for protocol swift (OVHcloud Object Storage with Keystone v3 authentication)
[swift]
user_name=openstack_user_name
password=openstack_password
auth_url=openstack_auth_url
domain=openstack_auth_url_domain
region=openstack_region

```

## Build
```
make init
make release
```

## Run
```
ovh-spark-submit [--jobname JOBNAME] [--region REGION] [--projectid PROJECTID] [--spark-version SPARK-VERSION] [--upload UPLOAD] [--class CLASS] [--driver-cores DRIVER-CORES] [--driver-memory DRIVER-MEMORY] [--driver-memoryOverhead DRIVER-MEMORYOVERHEAD] [--executor-cores EXECUTOR-CORES] [--num-executors NUM-EXECUTORS] [--executor-memory EXECUTOR-MEMORY] [--executor-memoryOverhead EXECUTOR-MEMORYOVERHEAD] [--packages PACKAGES] [--repositories REPOSITORIES] [--properties-file PROPERTIES-FILE] [--ttl TTL] [--conf CONF] [--job-conf JOB-CONF] FILE [PARAMETERS [PARAMETERS ...]]
                 
Positional arguments:
   FILE
   PARAMETERS
 
Options:
   --jobname JOBNAME      Job name (can be set with ENV vars JOB_NAME)
   --region REGION        Openstack region of the job (can be set with ENV vars OS_REGION) [default: GRA]
   --projectid PROJECTID
                          Openstack ProjectID (can be set with ENV vars OS_PROJECT_ID)
   --spark-version SPARK-VERSION
                          Version of spark (can be set with ENV vars SPARK_VERSION) [default: 2.4.3]
   --upload UPLOAD        Comma-delimited list of file path/dir to upload before running the job (can be set with ENV vars UPLOAD)
   --class CLASS          main-class
   --driver-cores DRIVER-CORES
   --driver-memory DRIVER-MEMORY
                          Driver memory in (gigi/mebi)bytes (eg. "10G")
   --driver-memoryOverhead DRIVER-MEMORYOVERHEAD
                          Driver memoryOverhead in (gigi/mebi)bytes (eg. "10G")
   --executor-cores EXECUTOR-CORES
   --num-executors NUM-EXECUTORS
   --executor-memory EXECUTOR-MEMORY
                          Executor memory in (gigi/mebi)bytes (eg. "10G")
   --executor-memoryOverhead EXECUTOR-MEMORYOVERHEAD
                          Executor memory in (gigi/mebi)bytes (eg. "10G")
   --packages PACKAGES    Comma-delimited list of Maven coordinates
   --repositories REPOSITORIES
                          Comma-delimited list of additional repositories (or resolvers in SBT)
   --properties-file      Read properties from the given file
   --ttl                  Maximum "Time To Live" (in RFC3339 (duration) eg. "P1DT30H4S") of this job, after which it will be automatically terminated
   --conf                 Allows you to set the path to your configuration.ini instead of the default one
   --job-conf             Allows you to use a configuration file for your job definition instead of the CLI options. Supports JSON and HJSON format.
   --help, -h             display this help and exit
                 

```

### Example

Without Auto Upload:
```
OS_PROJECT_ID=1377b21260f05b410e4652445ac7c95b  ./ovh-spark-submit --class org.apache.spark.examples.SparkPi --driver-cores 1 --driver-memory 4G --executor-cores 1 --executor-memory 4G --num-executors 1 swift://odp/spark-examples.jar 1000
```

With Auto Upload

```
OS_PROJECT_ID=1377b21260f05b410e4652445ac7c95b  ./ovh-spark-submit --upload ./spark-examples.jar --class org.apache.spark.examples.SparkPi --driver-cores 1 --driver-memory 4G --executor-cores 1 --executor-memory 4G --num-executors 1 swift://odp/spark-examples.jar 1000
```

With a job configuration file
Example of job.hjson :
```
{
  "jobname": "myAwesomeJob"
  "region": GRA
  "projectid": XXX
  "spark-version": 3.3.0
  "class": org.apache.spark.examples.SparkPi
  "driver-cores": 2
  "driver-memory": 1G
  "driver-memoryOverhead": 1G
  "executor-cores": 2
  "num-executors": 1
  "executor-memory": 1G
  "executor-memoryOverhead": 1G
  "ttl": PT30H
  "file": swift://example/spark-examples.jar
  "parameters": "10000, 15000"
  "upload": "spark-examples.jar"
}
```

Command : 
```
./ovh-spark-submit --job-conf job.hjson
```


### Outputs

Once your job is executed successfully, the CLI prints out jobs information:

- the jobs log location
- the exitcode is printed out
- the CLI exit with this code

```txt
2022-10-07 09:01:09,693 - deploy - INFO - End of job cc5724d1-bdce-4e99-a72f-xxxx with status 0
2022/10/07 11:01:12 You can download your logs at https://storage.gra.cloud.ovh.net/v1/AUTH_4beb99ff282e4d16b215375xxxx/odp-logs?prefix=cc5724d1-bdce-4e99-a72f-xxxx
2022/10/07 11:01:12 Job status is : COMPLETED
2022/10/07 11:01:12 Job exit code : 0
```
