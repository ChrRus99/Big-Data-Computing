import org.apache.spark.SparkConf;
import org.apache.spark.api.java.StorageLevels;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import scala.Tuple2;

import java.util.*;
import java.util.concurrent.Semaphore;
import java.util.stream.Collectors;

public class G028HW3 {
    // Number of items after which stop the execution of the program
    public static final int THRESHOLD = 10000000;       // 10M items

    // Global variables (static to avoid resetting between batches)
    private static long[] sigmaR_size = new long[1];    // SigmaR (filtered stream) length
    private static long[][] arrayC;                     // Array C of the count sketch algorithm

    public static void main(String[] args) throws Exception {
        // INPUT HANDLING
        if (args.length != 6) {
            throw new IllegalArgumentException("USAGE: D W left right K portExp");
        }

        SparkConf conf = new SparkConf(true)
                .setMaster("local[*]") // remove this line if running on the cluster
                .setAppName("G028HW3");

        // Here, with the duration you can control how large to make your batches.
        JavaStreamingContext sc = new JavaStreamingContext(conf, Durations.milliseconds(100));
        sc.sparkContext().setLogLevel("ERROR");

        // TECHNICAL DETAIL
        Semaphore stoppingSemaphore = new Semaphore(1);
        stoppingSemaphore.acquire();

        // INPUT READING
        // Read input parameters
        int D = Integer.parseInt(args[0]);          // Number of rows of the count sketch
        int W = Integer.parseInt(args[1]);          // Number of columns of the count sketch
        int left = Integer.parseInt(args[2]);       // Left endpoint of the interval of interest
        int right = Integer.parseInt(args[3]);      // Right endpoint of the interval of interest
        int K = Integer.parseInt(args[4]);          // Number of top frequent items of interest
        int portExp = Integer.parseInt(args[5]);    // Port number

        // DEFINING THE REQUIRED DATA STRUCTURES TO MAINTAIN THE STATE OF THE STREAM
        // Definition of the parameters for computing the hash functions h_j's and g_j's
        final int p = 8191;
        Random rand = new Random();
        List<Tuple2<Long, Long>> hash_params = new ArrayList<>();

        for(int i=0; i<2*D; i++) {
            long a = 1 + rand.nextInt((p - 1 - 1) + 1); // [1, p-1]
            long b = 0 + rand.nextInt((p - 1 - 0) + 1); // [0, p-1]

            hash_params.add(new Tuple2<>(a, b));
        }

        // Counters
        long[] streamLength = new long[1];                  // Stream length (an array to be passed by reference)
        streamLength[0]=0L;
        sigmaR_size[0]=0L;                                  // SigmaR (filtered stream) length
        arrayC = new long[D][W];                            // Array C of the count sketch algorithm
        int sigmaR_range = right - left + 1;                // Number of items in the SigmaR range

        // Frequencies
        HashMap<Long, Long> f_u_exact = new HashMap<>();    // Exact frequencies
        long[][] f_uj_approx = new long[sigmaR_range][D];   // Partial approximate frequencies
        HashMap<Long, Long> f_u_approx = new HashMap<>();   // Approximate frequencies

        // Second moments
        double F2_exact_norm = -1;                          // Exact second moment (normalized)
        long[] F2j_approx = new long[D];                    // Partial approximate second moment
        double F2_approx_norm = -1;                         // Approximate second moment (normalized)

        // CODE TO PROCESS AN UNBOUNDED STREAM OF DATA IN BATCHES
        sc.socketTextStream("algo.dei.unipd.it", portExp, StorageLevels.MEMORY_AND_DISK)
                // For each batch
                .foreachRDD((batch, time) -> {
                    // On the batch at time 'time'
                    if (streamLength[0] < THRESHOLD) {
                        long batchSize = batch.count();
                        streamLength[0] += batchSize;

                        /* Count sketch algorithm on the elements in Sigma_R */
                        batch
                                // Extract items of SigmaR in range [left, right]
                                .filter(item -> {
                                    long x_t = Long.parseLong(item);
                                    return left <= x_t && x_t <= right;
                                })
                                // For each item in SigmaR fill the arrayC table
                                .foreach(item -> {
                                    long x_t = Long.parseLong(item);
                                    sigmaR_size[0] += 1L;

                                    for(int j=0; j<D; j++) {
                                        long a_hj = hash_params.get(j)._1;
                                        long b_hj = hash_params.get(j)._2;

                                        long a_gj = hash_params.get(j+D)._1;
                                        long b_gj = hash_params.get(j+D)._2;

                                        int h_j = (int)(((a_hj * x_t + b_hj) % p) % W);                     // map the hash function in a range [0, W-1]
                                        long g_j = (int)(((a_gj * x_t + b_gj) % p) % 2) == 0L ? -1L : 1L;   // map the hash function in a range [0, 1] -> [-1, 1]

                                        arrayC[j][h_j] = arrayC[j][h_j] + g_j;
                                    }
                                });

                        /* Extract the exact frequencies of the elements in Sigma_R */
                        Map<Long, Long> batchItems = batch
                                // R1 - MAP PHASE
                                // item -> (item, 1L) if item is in SigmaR || item -> (item, 0L) if item is not in SigmaR
                                .mapToPair(item -> {
                                    long x_t = Long.parseLong(item);

                                    if (left <= x_t && x_t <= right) {
                                        return new Tuple2<>(x_t, 1L);
                                    } else {
                                        return new Tuple2<>(x_t, 0L);
                                    }
                                })
                                // R1 - SHUFFLE & GROUPING
                                // (item, 1L || 0L) -> (item, count_list)
                                .groupByKey()
                                // R1 - REDUCE PHASE
                                // (item, count_list) -> (item, counter)
                                .mapToPair(pair -> {
                                    long key = pair._1;
                                    Iterable<Long> countIterable = pair._2;

                                    List<Long> countList = new ArrayList<>();
                                    countIterable.forEach(countList::add);
                                    long sum = countList.stream().mapToLong(count -> count).sum();

                                    return new Tuple2<>(key, sum);
                                })
                                // Filter the items not in SigmaR (the ones whose counter is 0) and save result in a map
                                .filter(pair -> {
                                    return pair._2 != 0;
                                })
                                .collectAsMap();

                        // Update the streaming state
                        batchItems.forEach((key, value) -> {
                            if (!f_u_exact.containsKey(key)) {
                                f_u_exact.put(key, value);
                            } else {
                                f_u_exact.put(key, f_u_exact.get(key) + value);
                            }
                        });

                        if (streamLength[0] >= THRESHOLD) {
                            stoppingSemaphore.release();
                        }
                    }
                });

        // MANAGING STREAMING SPARK CONTEXT
        System.out.println("Starting streaming engine");
        sc.start();
        System.out.println("Waiting for shutdown condition");
        stoppingSemaphore.acquire();
        System.out.println("Stopping the streaming engine");
        sc.stop(false, false);
        System.out.println("Streaming engine stopped");

        // COMPUTE FINAL STATISTICS
        /* Compute f_u approximate */
        for(int u=left; u<=right; u++) { // for any u in Sigma_R
            for(int j=0; j<D; j++) {
                long a_hj = hash_params.get(j)._1;
                long b_hj = hash_params.get(j)._2;

                long a_gj = hash_params.get(j+D)._1;
                long b_gj = hash_params.get(j+D)._2;

                int h_j = (int)(((a_hj * u + b_hj) % p) % W);                       // map the hash function in a range [0, W-1]
                long g_j = (int)(((a_gj * u + b_gj) % p) % 2) == 0L ? -1L : 1L;     // map the hash function in a range [0, 1] -> [-1, 1]

                f_uj_approx[u-left][j] = g_j * arrayC[j][h_j];
            }
        }

        for(int u=left; u<=right; u++) {
            long[] ordered_f_u_approx = f_uj_approx[u-left];
            Arrays.sort(ordered_f_u_approx);
            f_u_approx.put((long)u, median(ordered_f_u_approx));
        }

        /* Compute the exact F2 (normalized) */
        F2_exact_norm = f_u_exact.values().stream().mapToDouble(frequency -> Math.pow(frequency.doubleValue(), 2.0)).sum() / Math.pow(sigmaR_size[0], 2.0);

        /* Compute the approximate F2 (normalized) */
        for(int j=0; j<D; j++) {
            for (int k=0; k<W; k++) {
                F2j_approx[j] += Math.pow(arrayC[j][k], 2.0);
            }
        }

        long[] ordered_F2j_approx = F2j_approx;
        Arrays.sort(ordered_F2j_approx);
        F2_approx_norm = median(ordered_F2j_approx) / Math.pow(sigmaR_size[0], 2.0);

        /* Compute the K most true frequent items */
        long phi_K = f_u_exact.values().stream().sorted(Comparator.reverseOrder()).collect(Collectors.toList()).get(K-1);
        Map<Long, Long> most_freq_f_u_exact = f_u_exact.entrySet().stream().filter(pair -> pair.getValue() >= phi_K).collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
        Map<Long, Long> most_freq_f_u_approx = new HashMap<>();
        most_freq_f_u_exact.keySet().stream().forEach(key -> {
            most_freq_f_u_approx.put(key, f_u_approx.get(key));
        });

        /* Compute the average relative error of the most frequent items */
        List<Double> relative_error_list = new ArrayList<>();

        for(long key : most_freq_f_u_exact.keySet()) {
             relative_error_list.add(Math.abs(most_freq_f_u_exact.get(key).doubleValue() - most_freq_f_u_approx.get(key).doubleValue()) / most_freq_f_u_exact.get(key).doubleValue());
        }

        double avg_relative_error = relative_error_list.stream().mapToDouble(err -> err).average().getAsDouble();

        // PRINT FINAL STATISTICS
        System.out.println("D = " + D + " W = " + W + " [left, right] = [" + left + "," + right + "]" + " K = " + K + " Port = " + portExp);
        System.out.println("Total number of items = " + streamLength[0]);
        System.out.println("Total number of items in [" + left + "," + right + "] = " + sigmaR_size[0]);
        System.out.println("Number of distinct items in [" + left + "," + right + "] = " + sigmaR_range);
        if(K <= 20) {
            for (long key : most_freq_f_u_exact.keySet()) {
                System.out.println("Item " + key + " Freq = " + most_freq_f_u_exact.get(key) + " Est. Freq = " + most_freq_f_u_approx.get(key));
            }
        }
        System.out.println("Avg err for top " + K + " = " + avg_relative_error);
        System.out.println("F2 " + F2_exact_norm + " F2 Estimate " + F2_approx_norm);
    }

    /**
     * Compute the median of an array of long values.
     * @param array ordered array of long values
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