# Use a more recent and secure base image
FROM rockylinux:8

ARG UID=1000
ARG PASSWORD

# Set default values for build arguments
ARG JAVA_VERSION=11
ARG ANACONDA_VERSION=2024.10-1
# For Mac M1/M2 compatibility, use 'x86_64' for Intel/AMD
ARG ARCH=aarch64  
ARG SPARK_VERSION=3.5.6
ARG HADOOP_VERSION=3
ARG KAFKA_VERSION=3.7.2
ARG KAFKA_SCALA_VERSION=2.12

# Install packages in a single layer to reduce image size
RUN dnf -y update && \
    dnf -y install epel-release && \
    dnf -y install \
        wget \
        dialog \
        curl \
        sudo \
        lsof \
        vim \
        telnet \
        nano \
        openssh-server \
        openssh-clients \
        bzip2 \
        passwd \
        tar \
        bc \
        git \
        unzip

# Install Java
RUN dnf -y install java-${JAVA_VERSION}-openjdk java-${JAVA_VERSION}-openjdk-devel

# Create guest user with UID 1000
RUN useradd -m -u $UID guest && \
    echo "guest:$PASSWORD" | chpasswd && \
    usermod -aG wheel guest

# Set environment variables
ENV HOME=/home/guest

# Set working directory and user
WORKDIR ${HOME}
USER guest

# Install Anaconda
RUN wget https://repo.anaconda.com/archive/Anaconda3-${ANACONDA_VERSION}-Linux-${ARCH}.sh -O anaconda.sh && \
    chmod +x anaconda.sh && \
    bash anaconda.sh -b -p $HOME/anaconda3 && \
    rm anaconda.sh


# Install Spark
RUN wget https://dlcdn.apache.org/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz && \
    tar xzf spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz && \
    mv spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION} spark && \
    rm spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz

# Install Kafka
RUN wget https://dlcdn.apache.org/kafka/${KAFKA_VERSION}/kafka_${KAFKA_SCALA_VERSION}-${KAFKA_VERSION}.tgz && \
    tar xvzf kafka_${KAFKA_SCALA_VERSION}-${KAFKA_VERSION}.tgz \
    && mv kafka_${KAFKA_SCALA_VERSION}-${KAFKA_VERSION} kafka \
    && rm kafka_${KAFKA_SCALA_VERSION}-${KAFKA_VERSION}.tgz

# Add environment variables
RUN echo "export SPARK_HOME=$HOME/spark" >> ${HOME}/.bashrc && \
    echo "export KAFKA_HOME=$HOME/kafka" >> ${HOME}/.bashrc && \
    echo "export PATH=$HOME/anaconda3/bin:$HOME/spark/bin:$HOME/spark/sbin:$HOME/kafka/bin:$PATH" >> ${HOME}/.bashrc

RUN PY4J_ZIP=$(ls $HOME/spark/python/lib/py4j-*-src.zip | head -n 1) && \
    echo "py4j_zip retrieved path: ${PY4J_ZIP}" && \
    echo "export PYTHONPATH=$HOME/spark/python:$HOME/spark/python/lib/pyspark.zip:$PY4J_ZIP:$PYTHONPATH" >> ${HOME}/.bashrc

# Install Kafka Python package
RUN ${HOME}/anaconda3/bin/pip install --upgrade pip && \
    ${HOME}/anaconda3/bin/pip install kafka-python 

# Add alias for notebook
RUN echo "alias notebook=\"jupyter notebook --ip='0.0.0.0' --NotebookApp.iopub_data_rate_limit=2147483647 --no-browser \" " >> ${HOME}/.bashrc

# Set as root user
USER root

# Startup Zookeeper and Kafka servers
ADD start_services.sh /usr/bin/start_services.sh
RUN chmod a+x /usr/bin/start_services.sh




