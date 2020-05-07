# OVHcloud Data Processing Spark-submit

[![Gitter](https://img.shields.io/gitter/room/nwjs/nw.js.svg)](https://gitter.im/ovh/data-processing)
[![travis](https://travis-ci.org/ovh/data-processing-spark-submit?branch=master)](https://travis-ci.org/ovh/data-processing-spark-submit)

With OVHcloud Data Processing, you can run your processing job over the cloud, in a fast, easy and cost-efficient way. 
Everything is managed and secured by OVHcloud and you just need to define how much resources you would like. 
You can learn more about OVHcloud Data Processing [here](https://docs.ovh.com/gb/en/data-processing/overview/).

In this repository you will find a spark-submit wrapper to run spark job on OVHcloud Data Processing. 

## Configuration

First Create an OVH token by visiting  https://eu.api.ovh.com/createToken/
and add right GET/POST/PUT on endpoint /cloud/project/\*/dataProcessing/\*

Then create the configuration file like below :

```ini
[default]
; general configuration: default endpoint
endpoint=ovh-eu

[ovh-eu]
; configuration specific to 'ovh-eu' endpoint
application_key=my_app_key
application_secret=my_application_secret
consumer_key=my_consumer_key
```

The client will successively attempt to locate this configuration file in:

1. Current working directory: ``./ovh.conf``
2. Current user's home directory ``~/.ovh.conf``
3. System wide configuration ``/etc/ovh.conf``




## Build
```
make init
make release
```

## Run
```
ovh-spark-submit [--jobname JOBNAME] [--region REGION] --projectid PROJECTID [--version VERSION] [--class CLASS] --driver-cores DRIVER-CORES --driver-memory DRIVER-MEMORY [--driver-memoryOverhead DRIVER-MEMORYOVERHEAD] --executor-cores EXECUTOR-CORES --num-executors NUM-EXECUTORS --executor-memory EXECUTOR-MEMORY [--executor-memoryOverhead EXECUTOR-MEMORYOVERHEAD] FILE [PARAMETERS [PARAMETERS ...]]
                 
Positional arguments:
   FILE
   PARAMETERS
 
Options:
   --jobname JOBNAME      Job name (can be set with ENV VARS JOB_NAME)
   --region REGION        Openstack region of the job (can be set with ENV VARS OS_REGION) [default: GRA]
   --projectid PROJECTID
                          Openstack ProjectID (can be set with ENV VARS OS_PROJECT_ID)
   --version VERSION      Version of spark (can be set with ENV VARS SPARK_VERSION) [default: 2.4.3]
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
   --help, -h             display this help and exit
```

### Example
```
OS_PROJECT_ID=1377b21260f05b410e4652445ac7c95b  ./ovh-spark-submit --class org.apache.spark.examples.SparkPi --driver-cores 1 --driver-memory 4G --executor-cores 1 --executor-memory 4G --num-executors 1 s3://odp/spark-examples.jar 1000
```

