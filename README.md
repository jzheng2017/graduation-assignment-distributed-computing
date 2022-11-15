# Explanation
## Application architecture
![](application%20architecture.png)
Two separate components exist in this architecture, namely, the coordinator and the worker. The coordinator has as the name says, the responsibility to 'coordinate' everything in the cluster. 
For instance the partitions have to be assigned to workers that register themselves. These partitions are used for assigning consumers to it, that is to say, consumer X has been assigned partition X.
Each worker is assigned a partition, which then means that if a consumer is assigned to a partition, the worker that is responsible for that partition has to run these assigned consumers. 
The consumers that run on the worker consume messages from topics that they are subscribed to and process them accordingly as specified in its consumer configuration. 
All these information (partition/consumer assignments, consumer configuration, etc) are stored in a shared key value store, namely, etcd.

## Kubernetes infrastructure
![](kubernetes%20infrastructure.png)
The Kubernetes cluster consists of 5 distinct components that run in pods, namely: Kafka, Zookeeper, Etcd, Coordinator and the Worker component. 
Kafka is the message queue implementation used for this proof of concept. Kafka is dependent on Zookeeper which it used for storing configuration data. 
Etcd is used mainly to facilitate communication between the Coordinator and the Worker. The Coordinator uses etcd to store partition/consumer assignments and to keep track of workers in the cluster. 
The Worker also uses etcd to listen to these assignments and constructs its own state according to these configuration data. 
In contrary to the other components, the worker is replicated many times, that is there are multiple pods running workers in the cluster. The number of workers scales with the amount of work there is.