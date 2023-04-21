import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.io.*;
import java.net.URI;
import java.util.*;
import java.util.stream.Collectors;

public class SparkKMeans {

	private static final String HDFS_ROOT_URL = "hdfs://localhost:9000";
	private static final String SEPARATOR = ",";

	public static void main(String[] args) throws IOException {

		// Validating and parsing arguments
		if (args.length != 2)
			throw new IllegalArgumentException("Invalid number of arguments entered.");
		String centroidsFilePath = args[0], dataInputPath = args[1];

		// Create a Java Spark Context and load input data
		long startTime = System.currentTimeMillis();
		SparkConf conf = new SparkConf().setMaster("local").setAppName("SparkKMeans");
		JavaSparkContext sc = new JavaSparkContext(conf);
		JavaRDD<String> data = sc.textFile(dataInputPath);

		//FileSystem fileSystem = FileSystem.get(URI.create(HDFS_ROOT_URL), new Configuration()); //UNCOMMENT IF RUNNING ON HADOOP

		do {
			final ArrayList<ArrayList<Double>> centroids = readCentroids(/*fileSystem*/ null, centroidsFilePath);

			// Mapping data samples to their nearest centroids
			JavaPairRDD<Integer, List<Double>> mappedToNearestCentroid = data.mapToPair( sample -> {
				List<Double> s = Arrays.stream(sample.trim().split(SEPARATOR))
						.map(Double::parseDouble).collect(Collectors.toList());
				int nearestCentroid = getNearestCentroid(s, centroids);
				return new Tuple2<>(nearestCentroid, s);
			});

			// Summing all the samples which are mapped together
			JavaPairRDD<Integer, List<Double>> sumCentroidsRDD
				= mappedToNearestCentroid.reduceByKey((s1, s2) -> {
					for (int i = 0 ; i < s1.size() ; i++) s1.set(i, s1.get(i) + s2.get(i));
					return s1;
			});

			// Averaging the summation to get the new centroids
			final Map<Integer, Long> countMap = mappedToNearestCentroid.countByKey();
			JavaRDD<List<Double>> newCentroidsRDD = sumCentroidsRDD.map(tuple -> {
				int key = tuple._1();
				List<Double> value = tuple._2();
				long divisor = countMap.get(key);
				for (int i = 0 ; i < value.size() ; i++) value.set(i, value.get(i)/divisor);
				return value;
			});

			final List<List<Double>> newCentroids = newCentroidsRDD.collect();
			if (newCentroids.equals(centroids)) break;
			writeCentroids(/*fileSystem*/ null , newCentroids, centroidsFilePath);
		} while(true);
		//fileSystem.close(); //UNCOMMENT IF RUNNING ON HADOOP
		System.out.println("Execution time in ms: " + (System.currentTimeMillis() - startTime));
	}

	/**
	 * Reads the centroids file and returns a list of it.
	 */
	private static ArrayList<ArrayList<Double>> readCentroids
		(FileSystem fileSystem, String centroidsFilePath) throws IOException {

		//Scanner scanner = new Scanner(fileSystem.open(new Path(centroidsFilePath))); //UNCOMMENT IF RUNNING ON HADOOP
		Scanner scanner = new Scanner(new File(centroidsFilePath));
		ArrayList<ArrayList<Double>> centroids = new ArrayList<>();
		while (scanner.hasNext())
			centroids.add(Arrays.stream(scanner.nextLine().trim().split(" "))
					.map(Double::parseDouble)
					.collect(Collectors.toCollection(ArrayList::new)));
		scanner.close();
		return centroids;
	}

	/**
	 * Returns the nearest centroid index to the given sample.
	 * It uses the Euclidean distance.
	 */
	private static int getNearestCentroid(List<Double> sample, ArrayList<ArrayList<Double>> centroids) {
		double minimumDistance = Double.MAX_VALUE;
		int minimumIndex = -1;
		for (int c = 0 ; c < centroids.size() ; c++) {
			ArrayList<Double> centroid = centroids.get(c);
			if (sample.size() != centroid.size())
				throw new InputMismatchException("Centroids and data samples sizes mismatches.");
			double currentDistance = 0;
			for (int i = 0 ; i < sample.size() ; i++)
				currentDistance += Math.pow((sample.get(i)- centroid.get(i)), 2);
			currentDistance = Math.sqrt(currentDistance);
			if (currentDistance < minimumDistance) {
				minimumDistance = currentDistance;
				minimumIndex = c;
			}
		}
		return minimumIndex;
	}

	/**
	 * Writes the new centroids in the file.
	 */
	private static void writeCentroids
		(FileSystem fileSystem, List<List<Double>> centroids, String centroidsFilePath) throws IOException {

		//FSDataOutputStream out = fileSystem.create(new Path(centroidsFilePath), true); // UNCOMMENT FOR HADOOP
		//BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(out)); // UNCOMMENT FOR HADOOP and use it instead of fileSystem.
		FileWriter fileWriter = new FileWriter(centroidsFilePath);
		centroids.forEach(centroid -> {
			centroid.forEach(value -> {
				try { fileWriter.write(value.toString() + " "); } catch (IOException e) { e.printStackTrace(); }
			});
			try { fileWriter.write("\n"); } catch (IOException e) { e.printStackTrace(); }
		});
		//bw.close(); //UNCOMMENT IF RUNNING ON HADOOP
		//out.close(); //UNCOMMENT IF RUNNING ON HADOOP
		fileWriter.close();
	}
}
