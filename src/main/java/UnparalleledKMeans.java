import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.io.IOException;
import java.util.*;
import java.util.stream.Collectors;

public class UnparalleledKMeans {

    public static void main(String[] args) throws IOException {
        long startTime = System.currentTimeMillis();
        ArrayList<ArrayList<Double>> initialCentroids = readCentroids(); // Reading initial centroids
        ArrayList<ArrayList<Double>> newCentroids = initialCentroids;
        do {
            initialCentroids = newCentroids;
            ArrayList<ArrayList<Double>> data = readData();
            HashMap<Integer, ArrayList<ArrayList<Double>>> hashMap = new HashMap<>();
            for (ArrayList<Double> sample : data) {
                int nearestCentroid = getNearestCentroid(sample, initialCentroids);
                hashMap.putIfAbsent(nearestCentroid, new ArrayList<>());
                hashMap.get(nearestCentroid).add(sample);
            }
            ArrayList<ArrayList<Double>> finalNewCentroids = new ArrayList<>();
            hashMap.forEach((key, value) -> {
                double[] newCentroid = new double[data.get(0).size()];
                value.forEach(sample -> {
                    for(int i = 0 ; i < data.get(0).size() ; i++) newCentroid[i] += sample.get(i);
                });
                finalNewCentroids.add(Arrays.stream(newCentroid)
                                 .boxed()
                                 .map(element -> element/value.size())
                                 .collect(Collectors
                                 .toCollection(ArrayList::new)));
            });
            newCentroids = finalNewCentroids;
        } while (! newCentroids.equals(initialCentroids));
        writeCentroids(newCentroids);
        System.out.println("Execution time in ms: " + (System.currentTimeMillis() - startTime));
    }

    private static ArrayList<ArrayList<Double>> readCentroids() throws FileNotFoundException {
        Scanner scanner = new Scanner(new File("centroids.txt"));
        ArrayList<ArrayList<Double>> centroids = new ArrayList<>();
        while (scanner.hasNext())
            centroids.add(Arrays.stream(scanner.nextLine().trim().split(" "))
                    .map(Double::parseDouble)
                    .collect(Collectors.toCollection(ArrayList::new)));
        scanner.close();
        return centroids;
    }

    private static ArrayList<ArrayList<Double>> readData() throws FileNotFoundException {
        Scanner scanner = new Scanner(new File("input/Iris.csv"));
        ArrayList<ArrayList<Double>> data = new ArrayList<>();
        while(scanner.hasNextLine())
            data.add(Arrays.stream(scanner.nextLine().split(","))
                            .map(Double::parseDouble)
                            .collect(Collectors
                            .toCollection(ArrayList::new)));
        scanner.close();
        return data;
    }

    private static void writeCentroids(ArrayList<ArrayList<Double>> centroids) throws IOException {
        FileWriter fileWriter = new FileWriter("centroids.txt");
        centroids.forEach(centroid -> {
            centroid.forEach(value -> {
                try { fileWriter.write(value.toString() + " "); } catch (IOException e) { e.printStackTrace(); }
            });
            try { fileWriter.write("\n"); } catch (IOException e) { e.printStackTrace(); }
        });
        fileWriter.close();
    }

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
