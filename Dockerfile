FROM confluentinc/c:7.3.1

RUN   confluent-hub install --no-prompt confluentinc/kafka-connect-s3:latest