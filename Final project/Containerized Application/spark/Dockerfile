FROM ubuntu:16.04
ADD . /load_data
WORKDIR /load_data

ENV SPARK_VERSION 2.4.0
ENV HADOOP_VERSION 2.7

#RUN apt-get update && apt-get install -y sudo && rm -rf /var/lib/apt/lists/*
RUN apt-get update
RUN apt-get install -y python 3.6
RUN alias python=python3
RUN apt-get install -y default-jdk
RUN apt-get install -y scala
RUN apt-get install -y git
RUN apt-get install -y wget
RUN apt-get install -y vim

#RUN apt-get -y install python3-pip
#RUN pip3 install --upgrade pip3
RUN alias pip=pip2

# get spark
RUN wget https://www-us.apache.org/dist/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz
RUN tar xvf spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz

RUN pip install --upgrade pip
RUN pip install -r requirements.txt
