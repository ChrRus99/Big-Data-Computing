import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;
import scala.Tuple3;

import java.io.IOException;
import java.util.*;
import java.util.stream.DoubleStream;

public class G028HW2 {
    public static void main(String[] args) throws IOException {
        // CHECKING NUMBER OF CMD LINE PARAMETERS
        // Parameters are: num_partitions, num_runs, <path_to_file>
        if (args.length != 3) {
            throw new IllegalArgumentException("USAGE: num_partitions num_runs file_path");
        }

        // SPARK SETUP
        SparkConf conf = new SparkConf(true).setAppName("G028HW1");
        JavaSparkContext sc = new JavaSparkContext(conf);
        sc.setLogLevel("WARN");

        // INPUT READING
        // Read number of colors and number of repetitions
        int C = Integer.parseInt(args[0]);
        int R = Integer.parseInt(args[1]);

        // Read input file and subdivide it into C random partitions
        String filePath = args[2];
        JavaRDD<String> docs = sc.textFile(filePath).repartition(C).cache();

        // Display the name of the current analyzed file
        String[] tokens = filePath.split("/");
        String fileName = tokens[tokens.length - 1];
        System.out.println("Dataset = " + fileName);

        // SETTING GLOBAL VARIABLES
        long numEdges = docs.count();
        System.out.println("Number of Edges = " + numEdges);
        System.out.println("Number of Colors = " + C);
        System.out.println("Number of Repetitions = " + R);

        // ALGORITHM 1: MR_ApproxTCwithNodeColors
        long[] numTrianglesRuns = new long[R];
        long medianNumTriangles = -1;

        long start = -1, end = -1;
        double[] timeRuns = new double[R];
        double averageTime = -1;

        System.out.println("Approximation through node coloring");
        for (int i = 0; i < R; i++) {
            start = System.nanoTime();
            numTrianglesRuns[i] = MR_ApproxTCwithNodeColors(docs, C);
            end = System.nanoTime();
            timeRuns[i] = (end - start) / 1e6;
        }

        medianNumTriangles = median(numTrianglesRuns);
        averageTime = DoubleStream.of(timeRuns).average().getAsDouble();
        System.out.println("- Number of triangles (median over " + R + " runs) = " + medianNumTriangles);
        System.out.println("- Running time (average over " + R + " runs) = " + averageTime + " ms");

        // ALGORITHM 2: MR_ApproxTCwithSparkPartitions
        double time = -1;
        long numTriangles = -1;

        System.out.println("Exact number through node coloring triplet");
        start = System.nanoTime();
        numTriangles = MR_ExactTC(docs, C);
        end = System.nanoTime();

        time = (end - start) / 1e6;
        System.out.println("- Number of triangles = " + numTriangles);
        System.out.println("- Running time = " + time + " ms");
    }

    /**
     * Implementation of algorithm 1.
     *
     * @param rawData the RDD of strings: [E1, E2, ..., En], with Ei edge
     * @param C       the number of colors/partitions
     * @return the final estimate of the number of triangles
     */
    public static long MR_ApproxTCwithNodeColors(JavaRDD<String> rawData, int C) {
        // Definition of the parameters for computing the hash function hC
        final int p = 8191;
        Random rand = new Random();
        int a = 1 + rand.nextInt((p - 1 - 1) + 1); // [1, p-1]
        int b = 0 + rand.nextInt((p - 1 - 0) + 1); // [0, p-1]

        // Operations on the RDD
        JavaPairRDD<Integer, Long> edges = rawData
                // R1 - MAP PHASE
                // Return pairs (color, edge=(u,v))
                // Extract a subset of edges whose vertices u, v have the same color
                .flatMapToPair((document) -> {  // document = a string: Ei = (u, v), with u, v vertices of the edge Ei
                    String[] edgeArray = document.split("\n");
                    ArrayList<Tuple2<Integer, Tuple2<Integer, Integer>>> pairs = new ArrayList<>();

                    for (String edge : edgeArray) {
                        // Calculate the two vertices of the edge
                        String[] vertices = edge.split(",");
                        int v1 = new Integer(vertices[0]);
                        int v2 = new Integer(vertices[1]);

                        // Calculate the color of each of the two vertices of the edge
                        int hC_v1 = ((a * v1 + b) % p) % C;
                        int hC_v2 = ((a * v2 + b) % p) % C;

                        // Fill the pairs list with the edges having the couple of vertices with the same color
                        if (hC_v1 == hC_v2) {
                            pairs.add(new Tuple2<>(hC_v1, new Tuple2<>(v1, v2)));
                        }
                    }

                    return pairs.iterator();
                })
                // R1 - SHUFFLE & GROUPING
                // Groups pairs per color and return pairs (color, list of edges)
                // Create C subsets of edges, where each edge Ei, with i in [0, C-1], is such that hC(u) = hC(v) = i
                .groupByKey()
                // R1 - REDUCE PHASE
                // Return the pairs (color, # triangles)
                // For each group of color i, compute the number of triangles in the relative list of edges
                .mapValues((edgesIterable) -> {
                    ArrayList<Tuple2<Integer, Integer>> edgeList = new ArrayList<>();
                    edgesIterable.forEach(edgeList::add);

                    return CountTriangles(edgeList);
                });

        // Calculate the final estimate of the total number of triangles in the graph G
        long sumTriangles = edges.collect().stream().mapToLong(pair -> pair._2).sum();
        long numTriangles = (int) Math.pow(C, 2) * sumTriangles;

        return numTriangles;
    }

    /**
     * Implementation of algorithm 2.
     *
     * @param rawData the RDD of strings: [E1, E2, ..., En], with Ei edge
     * @param C       the number of partitions
     * @return the exact number of triangles
     */
    public static long MR_ExactTC(JavaRDD<String> rawData, int C) { // TO MODIFY
        // Definition of the parameters for computing the hash function hC
        final int p = 8191;
        Random rand = new Random();
        int a = 1 + rand.nextInt((p - 1 - 1) + 1); // [1, p-1]
        int b = 0 + rand.nextInt((p - 1 - 0) + 1); // [0, p-1]

        // Operations on the RDD
        JavaPairRDD<Tuple3<Integer, Integer, Integer>, Long> edges = rawData
                // R1 - MAP PHASE
                // Return pairs ((hC(u), hC(v), i), edge=(u,v))
                // Assign to each edge in the document a color triplet (hC(u), hC(v), i)
                .flatMapToPair((document) -> {  // document = a string: Ei = (u, v), with u, v vertices of the edge Ei
                    String[] edgeArray = document.split("\n");
                    ArrayList<Tuple2<Tuple3<Integer, Integer, Integer>, Tuple2<Integer, Integer>>> pairs = new ArrayList<>();

                    for (String edge : edgeArray) {
                        // Calculate the two vertices of the edge
                        String[] vertices = edge.split(",");
                        int u = new Integer(vertices[0]);
                        int v = new Integer(vertices[1]);

                        // Calculate the color triplet of each of the two vertices of the edge
                        for(int i = 0; i < C; i++) {
                            // Calculate the 3 colors of each of the two vertices of the edge
                            int hC_u = ((a * u + b) % p) % C;
                            int hC_v = ((a * v + b) % p) % C;

                            // Sort the key's 3 colors in non-decreasing order
                            ArrayList<Integer> k = new ArrayList<>(Arrays.asList(hC_u, hC_v, i));
                            Collections.sort(k);

                            // Fill the pairs list
                            Tuple3<Integer, Integer, Integer> key = new Tuple3<>(k.get(0), k.get(1), k.get(2));
                            Tuple2<Integer, Integer> value = new Tuple2<>(u, v);
                            pairs.add(new Tuple2<>(key, value));
                        }
                    }

                    return pairs.iterator();
                })
                // R1 - SHUFFLE & GROUPING
                // Group pairs per color triplet and return pairs (color triplet, list of edges)
                // Create C subsets of edges, where each edge Ei, with i in [0, C-1]
                .groupByKey()
                // R1 - REDUCE PHASE
                // Return the pairs (color triplet, # triangles)
                // For each group of color i, compute the number of triangles in the relative list of edges
                .mapToPair((pairs) -> {
                    Tuple3<Integer, Integer, Integer> key = pairs._1;
                    ArrayList<Tuple2<Integer, Integer>> edgeList = new ArrayList<>();
                    pairs._2.forEach(edgeList::add);

                    return new Tuple2<>(key, CountTriangles2(edgeList, key, a, b, p, C));
                });

        // Calculate the final estimate of the total number of triangles in the graph G
        long numTriangles = edges.collect().stream().mapToLong(pair -> pair._2).sum();

        return numTriangles;
    }

    /**
     * Count the number of triangles in an undirected graph, given the set of its edges.
     *
     * @param edgeSet the set of edges of the undirect graph
     * @return the number of triangles in the undirect graph
     */
    public static Long CountTriangles(ArrayList<Tuple2<Integer, Integer>> edgeSet) {
        if (edgeSet.size() < 3) return 0L;
        HashMap<Integer, HashMap<Integer, Boolean>> adjacencyLists = new HashMap<>();
        for (int i = 0; i < edgeSet.size(); i++) {
            Tuple2<Integer, Integer> edge = edgeSet.get(i);
            int u = edge._1();
            int v = edge._2();
            HashMap<Integer, Boolean> uAdj = adjacencyLists.get(u);
            HashMap<Integer, Boolean> vAdj = adjacencyLists.get(v);
            if (uAdj == null) {
                uAdj = new HashMap<>();
            }
            uAdj.put(v, true);
            adjacencyLists.put(u, uAdj);
            if (vAdj == null) {
                vAdj = new HashMap<>();
            }
            vAdj.put(u, true);
            adjacencyLists.put(v, vAdj);
        }
        Long numTriangles = 0L;
        for (int u : adjacencyLists.keySet()) {
            HashMap<Integer, Boolean> uAdj = adjacencyLists.get(u);
            for (int v : uAdj.keySet()) {
                if (v > u) {
                    HashMap<Integer, Boolean> vAdj = adjacencyLists.get(v);
                    for (int w : vAdj.keySet()) {
                        if (w > v && (uAdj.get(w) != null)) numTriangles++;
                    }
                }
            }
        }
        return numTriangles;
    }

    /**
     * Count the number of triangles in an undirected graph, given the set of its edges.
     *
     * @param edgeSet the set of edges of the undirect graph
     * @param key the key associated to the edgeSet
     * @param a the parameter a used for the hash color function
     * @param b the parameter b used for the hash color function
     * @param p the parameter p used for the hash color function
     * @param C the number of colors to use
     * @return the number of triangles in the undirect graph
     */
    public static Long CountTriangles2(ArrayList<Tuple2<Integer, Integer>> edgeSet, Tuple3<Integer, Integer, Integer> key, long a, long b, long p, int C) {
        if (edgeSet.size()<3) return 0L;
        HashMap<Integer, HashMap<Integer,Boolean>> adjacencyLists = new HashMap<>();
        HashMap<Integer, Integer> vertexColors = new HashMap<>();
        for (int i = 0; i < edgeSet.size(); i++) {
            Tuple2<Integer,Integer> edge = edgeSet.get(i);
            int u = edge._1();
            int v = edge._2();
            if (vertexColors.get(u) == null) {vertexColors.put(u, (int) ((a*u+b)%p)%C);}
            if (vertexColors.get(v) == null) {vertexColors.put(v, (int) ((a*v+b)%p)%C);}
            HashMap<Integer,Boolean> uAdj = adjacencyLists.get(u);
            HashMap<Integer,Boolean> vAdj = adjacencyLists.get(v);
            if (uAdj == null) {uAdj = new HashMap<>();}
            uAdj.put(v,true);
            adjacencyLists.put(u,uAdj);
            if (vAdj == null) {vAdj = new HashMap<>();}
            vAdj.put(u,true);
            adjacencyLists.put(v,vAdj);
        }
        Long numTriangles = 0L;
        for (int u : adjacencyLists.keySet()) {
            HashMap<Integer,Boolean> uAdj = adjacencyLists.get(u);
            for (int v : uAdj.keySet()) {
                if (v>u) {
                    HashMap<Integer,Boolean> vAdj = adjacencyLists.get(v);
                    for (int w : vAdj.keySet()) {
                        if (w>v && (uAdj.get(w)!=null)) {
                            ArrayList<Integer> tcol = new ArrayList<>();
                            tcol.add(vertexColors.get(u));
                            tcol.add(vertexColors.get(v));
                            tcol.add(vertexColors.get(w));
                            Collections.sort(tcol);
                            boolean condition = (tcol.get(0).equals(key._1())) && (tcol.get(1).equals(key._2())) && (tcol.get(2).equals(key._3()));
                            if (condition) {numTriangles++;}
                        }
                    }
                }
            }
        }
        return numTriangles;
    }

    /**
     * Compute the median of an array of long values.
     * @param array array of long values
     * @return the median
     */
    private static long median(long[] array) {
        int middle = array.length / 2;
        if (array.length % 2 == 1) {
            return array[middle];
        } else {
            return (array[middle - 1] + array[middle]) / 2;
        }
    }
}