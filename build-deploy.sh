#!/bin/bash
eval $(minikube docker-env)

#docker rmi consumer-coordinator
#docker rmi message-queue-kafka-project

cd consumer-coordinator
docker build --no-cache -t consumer-coordinator .

cd ../worker
docker build --no-cache -t worker .

cd ..
kubectl apply -f kubernetes/