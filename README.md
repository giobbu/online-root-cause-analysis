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

### Docker Configuration Details (TODO)

### Dockerfile

### Docker-Compose 
* Services
* Volume