![Status](https://img.shields.io/badge/status-development-orange)
[![License: GPL v3](https://img.shields.io/badge/License-GPLv3-blue.svg)](https://www.gnu.org/licenses/gpl-3.0)
# Online Root Cause Analysis

## Docker Setup

### Installation

Check if Docker Compose is installed:
```bash
docker-compose -v
```
If not installed, follow the [Docker Getting Started Docs](https://www.docker.com/get-started/).

### Built Image

To build the image:

```bash
docker-compose build
```

Check that the image has been successfully pulled:

```bash
docker image list
```

Example output:

```bash
REPOSITORY               TAG       IMAGE ID       CREATED          SIZE
giobbu/kafka-spark-app   latest    <containerId>  About an hour ago  10.6GB
```
### Run the Application

To start the container:

```bash
docker-compose run --rm app
```
* `--rm`: flag to remove the container after it stops.

### Start Zookeper and Kafka services
Logged as root user in `usr/bin` start kafka server

```bash
start_services.sh
```

Check `kafka.log` and `zookeeper.log` are in `home/guest`

```bash
anaconda3  kafka  kafka.log  notebooks  spark  zookeeper.log
```


### Spark Installation

In `home/guest`

```bash
pyspark --version
```

Output

```bash
Welcome to
      ____              __
     / __/__  ___ _____/ /__
    _\ \/ _ \/ _ `/ __/  '_/
   /___/ .__/\_,_/_/ /_/\_\   version 3.5.6
      /_/
                        
Using Scala version 2.12.18, OpenJDK 64-Bit Server VM, 11.0.25
```
### Start Jupyter Notebook

Switch to guest user
```bash
su guest
```
And start jupyter notebook

```bash
notebook
```
Connect to ` http://127.0.0.1:8889/tree?token=<token>`

### Docker Configuration Details (TODO)

### Dockerfile

### Docker-Compose 
* Services
* Volume