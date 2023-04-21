# Parallel K-Means

This assignment was part of **CS432: Distributed Systems** and it aims to enhance your understanding of Apache Hadoop, also gaining 
experience with Spark and MapReduce programming framework for large-scale data processing.

In this repository you will find implementation of the K-Means algorithm for clustering using MapReduce, Spark and the unparalleled verison
of it. You can configure it with any number of features or clusters. In the MapReduce or Spark code you can run it on IDE by creating a maven 
project with the corresponding dependencies as mentioned in the pom.xml file. Also you can run it on hadoop (HDFS) but you will need 
to uncomment some lines mentioned in the source code to handle HDFS file system. After the execution, the final centroids for clustering are 
overwrtitten in the same initial centroids text file.

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
(initially entered by the user). And it serializes it using gson library and sends them in the configurations to be accessed by all mappers and
reducers.
2. Then comes the mapper job. It reads the input file and deserializes the centroids and finds
the closest centroid to each sample of the data and emits this sample
with the index of the nearest centroid.
3. Then the each reducer gets all the data samples which are the closest to a
certain centroid so it averages all of them and updates the centroid values.
4. Then, we get back to the main function where it writes the new
centroids in the same file it has already read from it and it deletes the
output directory created by the mapreduce (to be able to run again). And it
checks whether the centroids values are changed or not. If not it breaks the
execution. And if they are changed it will create new map-reduce job and so on.
5. Finally, The code outputs the final centroids after convergence in the same file it reads from.

The code is well documented where you will find each step as discussed above.

## Spark

The code is very similar to the mapreduce part except that all the data
are stored and processed as JavaPairRDD. However, here are the steps:

1) Map all the points which are close to a certain centroid together.
2) Reduce all the mapped data samples together by summing up all their values.
3) Divide each entry by the count of this group to get the new centroids.
4) Collect the new centroids and check if they have changed. If changed write them in the same centroids file. Otherwise, break the execution.

## How to run:

In this section, I'll show how to run the code on hadoop.

1. First, define the list of initial centroids in a file with this format:
```
a1 a2 a3 ... an -> centroid_1
a1 a2 a3 ... an -> centroid_2
a1 a2 a3 ... an -> centroid_3
          .
          .
          .
          v
      centroid_m
```
where **n** is the number of features and **m** is the number of clusters (k parameter).

2. Second, preprocess the data to be clustered by removing all unnecessary columns 
leaving only numeric columns and define the separator in code (the separator is “,” in case of csv files).

3. Upload the centroids file along with the data (to be clustered) on HDFS.

4. Modify the source code either for Spark or MapReduce by uncommenting the lines indicated in the code and
use the fileSystem instance (replace the nulls with the fileSystem instance).

5. Compile the code to generate the jars as following (jars are available in the repository):
    * For ***MapReduce*** use the following commands:<br>
    ```
    $HADOOP_HOME/bin/hadoop com.sun.tools.javac.Main MapReduceKMeans.java
    jar cf kmeans.jar MapReduceKMeans*.class
    ```
    * For ***Spark*** go to the maven project directory and build the project using the following command and the jar will be generated in the target 
    directory:<br>
    ```
    mvn clean install
    ```

6. Run the jar on hadoop using the following command:
    * For ***MapReduce*** use the following commands:<br>
    ```
    $HADOOP_HOME/bin/hadoop jar MapReduceKMeans.jar MapReduceKMeans <centroids_file_path> <input_directory_path>
    ```
    * For ***Spark*** use the following commands: (you should have [Apache Spark](https://spark.apache.org/downloads.html) installed and exists in your PATH)<br>
    ```
    spark-submit \
    --class SparkKMeans \
    --master local \
    SparkKMeans-1.0.jar \
    hdfs://localhost:9000/<centroids_file_path> \
    hdfs://localhost:9000/<input_directory_path>
    ```

## Conclusion

The final centroids of the unparalleled, map-reduce (IDE/Hadoop), Spark and scikit-learn outputs the same centroids after finishing execution.<br>
The map-reduce & Spark parallel version would work efficiently on a large
dataset. But if the data is small, the overhead of creating jobs and doing the map, shuffle
and reduce jobs or spark jobs would overcome the time of processing the data sequentially (the unparallel version). The execution of Spark is much faster than the map-reduce.
