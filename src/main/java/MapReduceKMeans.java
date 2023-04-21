import com.google.gson.Gson;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.*;
import java.net.URI;
import java.util.*;
import java.util.stream.Collectors;

public class MapReduceKMeans {

    private static final String HDFS_ROOT_URL = "hdfs://localhost:9000";
    private static final String HDFS_OUTPUT_TMP_FILE = "./tmp_1085";
    private static final String SEPARATOR = ",";

    /**
     * Mapper class which takes the data (samples) in text format and emits the nearest
     * centroid index to this sample where the key is the index and the value is the sample.
     */
    public static class KMeansMapper extends Mapper<Object, Text, IntWritable, Text> {

        private final IntWritable intWritable = new IntWritable();
        private final Text text = new Text();
        private final Gson gson = new Gson();

        /**
         * Mapper function.
         */
        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            Configuration configuration = context.getConfiguration();
            ArrayList<ArrayList<Double>> centroids = gson.fromJson
                    (configuration.get("centroids"), ArrayList.class);
            List<Double> sample = Arrays.stream(value.toString().trim().split(configuration.get("separator")))
                    .map(Double::parseDouble).collect(Collectors.toList());
            intWritable.set(getNearestCentroid(sample, centroids));
            text.set(gson.toJson(sample));
            context.write(intWritable, text);
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
    }

    /**
     * Reducer class which takes all the samples which are the nearest to a certain
     * centroid and updates the centroid accordingly.
     */
    public static class KMeansReducer extends Reducer<IntWritable, Text, Text, Text> {

        private final Gson gson = new Gson();

        /**
         * Reducer function.
         */
        @Override
        public void reduce(IntWritable key, Iterable<Text> values, Context context)
            throws IOException, InterruptedException {

            Configuration configuration = context.getConfiguration();
            int numberOfFeatures = Integer.parseInt(configuration.get("numberOfFeatures"));
            double[] newCentroid = new double[numberOfFeatures];
            int samples = 0;
            for (Text value : values) {
                ArrayList<Double> sample = gson.fromJson(value.toString(), ArrayList.class);
                for (int i = 0 ; i < numberOfFeatures ; i++) newCentroid[i] += sample.get(i);
                samples++;
            }
            for (int i = 0 ; i < numberOfFeatures ; i++) newCentroid[i] /= samples;
            StringBuilder sb = new StringBuilder(numberOfFeatures);
            Arrays.stream(newCentroid).forEach(value -> sb.append(value).append(" "));
            context.write(new Text(), new Text(sb.toString()));
        }
    }

    /**
     * Driver code.
     * arg1-> Path of the initial centroids file.
     * arg2-> Path of input data directory (or file).
     */
    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
        // Validating and parsing arguments
        if (args.length != 2)
            throw new IllegalArgumentException("Invalid number of arguments entered.");
        String centroidsFilePath = args[0], dataInputPath = args[1];

        // Initializing the configurations and reading the initial centroids
        long startTime = System.currentTimeMillis();
        Gson gson = new Gson();
        Configuration conf = new Configuration();
        ArrayList<ArrayList<Double>> centroids, newCentroids;
        //FileSystem fileSystem = FileSystem.get(URI.create(HDFS_ROOT_URL), conf); // UNCOMMENT IF RUNNING ON HADOOP
        do {
            centroids = readCentroids(/*fileSystem*/ null, centroidsFilePath);
            conf.set("centroids", gson.toJson(centroids, ArrayList.class));
            conf.set("numberOfFeatures", String.valueOf(centroids.get(0).size()));
            conf.set("separator", SEPARATOR);

            // Initializing a KMeans iteration job.
            Job job = Job.getInstance(conf, "MapReduceKMeans");
            job.setJarByClass(MapReduceKMeans.class);

            job.setMapperClass(KMeansMapper.class);
            job.setMapOutputKeyClass(IntWritable.class);
            job.setMapOutputValueClass(Text.class);

            job.setReducerClass(KMeansReducer.class);
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(Text.class);

            FileInputFormat.addInputPath(job, new Path(dataInputPath));
            FileOutputFormat.setOutputPath(job, new Path(HDFS_OUTPUT_TMP_FILE));
            job.waitForCompletion(true);

            newCentroids = readCentroids(null, HDFS_OUTPUT_TMP_FILE + "/part-r-00000");
            writeCentroids(null, newCentroids, centroidsFilePath);
            deleteOutputDirectory(null, HDFS_OUTPUT_TMP_FILE);
        } while(! newCentroids.equals(centroids));
        //fileSystem.close(); // UNCOMMENT IF RUNNING ON HADOOP
        System.out.println("Execution time in ms: " + (System.currentTimeMillis() - startTime));
    }

    //
    // Helper functions
    //

    /**
     * Reads the centroids file and returns a list of it.
     */
    private static ArrayList<ArrayList<Double>> readCentroids
        (FileSystem fileSystem, String centroidsFilePath) throws IOException {

        //Scanner scanner = new Scanner(fileSystem.open(new Path(centroidsFilePath))); // UNCOMMENT IF RUNNING ON HADOOP
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
     * Writes the new centroids in the file.
     */
    private static void writeCentroids
        (FileSystem fileSystem, ArrayList<ArrayList<Double>> centroids, String centroidsFilePath) throws IOException {

        //FSDataOutputStream out = fileSystem.create(new Path(centroidsFilePath), true); // UNCOMMENT FOR HADOOP
        //BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(out)); // UNCOMMENT FOR HADOOP AND USE IT INSTEAD OF fileWriter
        FileWriter fileWriter = new FileWriter(centroidsFilePath);
        centroids.forEach(centroid -> {
            centroid.forEach(value -> {
                try { fileWriter.write(value.toString() + " "); } catch (IOException e) { e.printStackTrace(); }
            });
            try { fileWriter.write("\n"); } catch (IOException e) { e.printStackTrace(); }
        });
        fileWriter.close();
        //bw.close(); // UNCOMMENT FOR HADOOP
        //out.close(); // UNCOMMENT FOR HADOOP
    }

    /**
     * Deletes the output directory.
     */
    private static void deleteOutputDirectory(FileSystem fileSystem, String outputPath) throws IOException {
        //fileSystem.delete(new Path(outputPath), true); // UNCOMMENT FOR HADOOP
        FileUtils.deleteDirectory(new File(outputPath));
    }
}
