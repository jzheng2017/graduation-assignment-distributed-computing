#!/bin/bash
eval $(minikube docker-env)

#docker rmi consumer-coordinator
#docker rmi message-queue-kafka-project

cd consumer-coordinator
docker build --no-cache -t consumer-coordinator .

cd ../message-queue-kafka-project
docker build --no-cache -t message-queue-kafka-project .

cd ..
kubectl apply -f kubernetes/