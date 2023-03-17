# K-Means

This assignment was part of **CS432: Distributed Systems** and it aims to enhance your understanding of Apache Hadoop, also gaining 
experience with the MapReduce programming framework for large-scale data processing.

In this repository you will find implementation of the K-Means algorithm for clustering using MapReduce and the unparalleled verison
of it. In the MapReduce code you can run it on IDE by creating a maven project with the corresponding dependencies as mentioned in the
pom.xml file. Also you can run it on hadoop (HDFS) but you will need to uncomment some lines mentioned in the source code to handle
hdfs file system.

## Description

A very common task in data analysis is grouping a set of unlabeled data such that all elements 
within a group are more similar among them than they are to the others. This falls under the 
field of unsupervised learning. Unsupervised learning techniques are widely used in several 
practical applications, e.g. analyzing the GPS data reported by a user to identify her most 
frequent visited locations. For any set of unlabeled observations, clustering algorithms tries to 
find the hidden structures in the data.

With the development of information technology, data volumes processed by many applications 
will routinely cross the peta-scale threshold, which would in turn increase the computational 
requirements. Efficient parallel clustering algorithms and implementation techniques are the 
key to meeting the scalability and performance requirements entailed in such scientific data 
analyses.

The Hadoop and the MapReduce programming model represents an easy framework to process 
large amounts of data where you can just implement the map and reduce functions and the 
underlying system will automatically parallelize the computations across large-scale clusters, 
handling machine failures, inter-machine communications, etc.
In this assignment you are asked to make a parallel version of the well-known and commonly 
used K-Means clustering algorithm using the map-reduce framework.
