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

## MapReduce

In this section, I’m going to explain how the MapReduce code works:

1. First, we start execution in the main function. The code reads the initial centroids
(initially entered by the user). And it parses it to a list and it sends them in the configurations to be accessed by all mappers and
reducers.
2. Then comes the mapper job. It reads the input file and It finds
the closest centroid to each sample of the data and it emits this sample
with the index of the nearest centroid.
3. Then the each reducer gets all the data samples which are the closest to a
certain centroid so it averages all of them and updates the centroid values.
4. Finally, we get back to the main function where it writes the new
centroids in the same file it has already read from it and it deletes the
output directory created by the mapreduce (to be able to run again). And it
checks whether the centroids values are changed or not. If not it breaks the
execution.

The code is well documented where you will find each step as discussed above.
