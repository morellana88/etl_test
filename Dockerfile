# AUTHOR: Marco Orellana
# DESCRIPTION: Technical test for ETL position at Applaudo Studio
# SOURCE: https://github.com/morellana88/etl_test

FROM python:3.7-slim-buster

ARG GCP_PROJECT_ID
ARG GCS_BUCKET
ARG BQ_DATASET
ARG GCP_SERVICE_ACCOUNT
ARG LOG_LEVEL

ENV LOG_LEVEL LOG_LEVEL
ENV LOG_FORMAT %(asctime)s %(levelname)s %(threadName)s %(name)s %(message)s
ENV GCP_PROJECT_ID $GCP_PROJECT_ID
ENV GCS_BUCKET $GCP_PROJECT_ID
ENV BQ_DATASET $BQ_DATASET
# Set the authorization variable using the service account passed from the arguments
ENV GOOGLE_APPLICATION_CREDENTIALS $GCP_SERVICE_ACCOUNT
ENV DEBIAN_FRONTEND noninteractive
ENV TERM linux

# Install gcc compiler
RUN apt-get update && \
    apt-get -y install gcc mono-mcs && \
    rm -rf /var/lib/apt/lists

# Copy project files
COPY . /etl_test
COPY ${GCP_SERVICE_ACCOUNT} .
WORKDIR /etl_test

# Install python dependencies
RUN pip install -r requirements.txt

# Create volume to process the local files
RUN mkdir /data
