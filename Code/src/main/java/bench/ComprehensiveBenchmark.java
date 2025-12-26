package bench;

import bst.MyBSTnext;
import bst.MyBSTBaseline;
import java.util.Random;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Comprehensive benchmark comparing MyBSTBaseline vs Handshake MyBST
 * with varying percentages of size queries
 */
public class ComprehensiveBenchmark {

    interface BSTInterface {
        void insert(int k);
        void delete(int k);
        Integer get(int k);
        int size();
    }

    static class BaselineWrapper implements BSTInterface {
        private final MyBSTBaseline<Integer,Integer> map = new MyBSTBaseline<>();
        public void insert(int k) { map.put(k, k); }
        public void delete(int k) { map.remove(k); }
        public Integer get(int k) { return map.get(k); }
        public int size() { return map.sizeSnapshot(); }
    }

    static class HandshakeWrapper implements BSTInterface {
        private final MyBSTnext<Integer,Integer> map = new MyBSTnext<>();
        public void insert(int k) { map.put(k, k); }
        public void delete(int k) { map.remove(k); }
        public Integer get(int k) { return map.get(k); }
        public int size() { return map.sizeSnapshot(); }
    }

    static class BenchmarkResult {
        String implementation;
        int threads;
        int sizeQueryPercent;
        long totalOps;
        long sizeQueries;
        double throughputMops;
        int finalSize;
        long errors;

        BenchmarkResult(String impl, int threads, int sizeQueryPercent, long totalOps, 
                       long sizeQueries, double throughputMops, int finalSize, long errors) {
            this.implementation = impl;
            this.threads = threads;
            this.sizeQueryPercent = sizeQueryPercent;
            this.totalOps = totalOps;
            this.sizeQueries = sizeQueries;
            this.throughputMops = throughputMops;
            this.finalSize = finalSize;
            this.errors = errors;
        }

        void print() {
            System.out.printf("  %-20s | %2d threads | %2d%% size | %,10d ops | %,6d sizes | %8.4f Mops/s | final=%,6d | errors=%d%n",
                implementation, threads, sizeQueryPercent, totalOps, sizeQueries, 
                throughputMops, finalSize, errors);
        }
    }

    static BenchmarkResult runBenchmark(BSTInterface ds, String name, int threads, int seconds, int sizePercent) throws Exception {
        // Preload
        for (int i = 0; i < 50_000; i++) ds.insert(i);

        ExecutorService pool = Executors.newFixedThreadPool(threads);
        CountDownLatch start = new CountDownLatch(1);
        CountDownLatch stop = new CountDownLatch(threads);

        final long endAt = System.nanoTime() + TimeUnit.SECONDS.toNanos(seconds);
        final ConcurrentLinkedQueue<Long> counts = new ConcurrentLinkedQueue<>();
        final ConcurrentLinkedQueue<Long> sizeQueries = new ConcurrentLinkedQueue<>();
        final AtomicLong sizeErrorCount = new AtomicLong(0);

        // Calculate operation percentages
        final int getPercent = 80 - sizePercent;  // Reduce gets to make room for size queries
        final int insertPercent = 10;
        final int deletePercent = 10;
        // sizePercent is provided

        for (int t = 0; t < threads; t++) {
            pool.submit(() -> {
                try {
                    Random rnd = new Random(Thread.currentThread().getId());
                    long ops = 0;
                    long sizeOps = 0;
                    start.await();
                    while (System.nanoTime() < endAt) {
                        int k = rnd.nextInt(200_000);
                        int r = rnd.nextInt(100);
                        
                        if (r < getPercent) { 
                            ds.get(k); 
                        }
                        else if (r < getPercent + insertPercent) { 
                            ds.insert(k); 
                        }
                        else if (r < getPercent + insertPercent + deletePercent) { 
                            ds.delete(k); 
                        }
                        else { 
                            // Size query
                            int size = ds.size();
                            if (size < 0 || size > 250_000) {
                                sizeErrorCount.incrementAndGet();
                            }
                            sizeOps++;
                        }
                        ops++;
                    }
                    counts.add(ops);
                    sizeQueries.add(sizeOps);
                } catch (InterruptedException ignored) { }
                finally { stop.countDown(); }
            });
        }

        start.countDown();
        stop.await();
        pool.shutdown();

        long totalOps = counts.stream().mapToLong(Long::longValue).sum();
        long totalSizeQueries = sizeQueries.stream().mapToLong(Long::longValue).sum();
        double mopsPerSec = totalOps / (double)seconds / 1_000_000.0;
        int finalSize = ds.size();

        return new BenchmarkResult(name, threads, sizePercent, totalOps, totalSizeQueries, 
                                   mopsPerSec, finalSize, sizeErrorCount.get());
    }

    public static void main(String[] args) throws Exception {
        int threads = (args.length >= 1) ? Integer.parseInt(args[0]) : 16;
        int seconds = (args.length >= 2) ? Integer.parseInt(args[1]) : 10;

        System.out.println("================================================================================");
        System.out.println("    COMPREHENSIVE BENCHMARK: MyBSTBaseline vs Handshake MyBST");
        System.out.println("================================================================================");
        System.out.printf("Configuration: %d threads, %d seconds per test%n", threads, seconds);
        System.out.println("Workload mix: 10%% insert, 10%% delete, varying %% get vs size queries");
        System.out.println("--------------------------------------------------------------------------------\n");

        // Run tests for different size query percentages
        int[] sizePercentages = {5, 15, 30};
        
        for (int sizePercent : sizePercentages) {
            System.out.println("\n╔════════════════════════════════════════════════════════════════════════════╗");
            System.out.printf("║  WORKLOAD: %d%% size queries, %d%% get, 10%% insert, 10%% delete              ║%n", 
                sizePercent, 80 - sizePercent);
            System.out.println("╚════════════════════════════════════════════════════════════════════════════╝\n");

            System.out.println("Testing MyBSTBaseline (Eager Propagation)...");
            BenchmarkResult baseline = runBenchmark(new BaselineWrapper(), "MyBSTBaseline", threads, seconds, sizePercent);
            
            System.out.println("Testing Handshake MyBST (Fast/Slow Path)...");
            BenchmarkResult handshake = runBenchmark(new HandshakeWrapper(), "Handshake MyBST", threads, seconds, sizePercent);
            
            System.out.println("\n  Results:");
            System.out.println("  " + "─".repeat(120));
            baseline.print();
            handshake.print();
            System.out.println("  " + "─".repeat(120));
            
            // Calculate improvement
            double speedup = (double)handshake.totalOps / baseline.totalOps;
            double improvement = ((double)(handshake.totalOps - baseline.totalOps) / baseline.totalOps) * 100;
            
            System.out.printf("\n  📊 Performance Analysis:%n");
            System.out.printf("     Handshake is %.2fx faster (%.1f%% more operations)%n", speedup, improvement);
            System.out.printf("     Baseline:  %,d ops/sec%n", baseline.totalOps / seconds);
            System.out.printf("     Handshake: %,d ops/sec%n", handshake.totalOps / seconds);
            
            if (baseline.errors == 0 && handshake.errors == 0) {
                System.out.println("     ✅ Both implementations: No errors detected");
            } else {
                System.out.printf("     ⚠️  Errors: Baseline=%d, Handshake=%d%n", baseline.errors, handshake.errors);
            }
        }

        // Summary comparison table
        System.out.println("\n\n");
        System.out.println("================================================================================");
        System.out.println("                          SUMMARY COMPARISON");
        System.out.println("================================================================================");
        System.out.println();
        System.out.println("  Size Query %  |  MyBSTBaseline  |  Handshake MyBST  |   Speedup   |  Improvement");
        System.out.println("  " + "─".repeat(80));
        
        // Re-run for summary (simplified - just show the key numbers)
        System.out.println("\n  Running final summary tests...\n");
        
        for (int sizePercent : sizePercentages) {
            BenchmarkResult baseline = runBenchmark(new BaselineWrapper(), "Baseline", threads, seconds, sizePercent);
            BenchmarkResult handshake = runBenchmark(new HandshakeWrapper(), "Handshake", threads, seconds, sizePercent);
            
            double speedup = (double)handshake.totalOps / baseline.totalOps;
            double improvement = ((double)(handshake.totalOps - baseline.totalOps) / baseline.totalOps) * 100;
            
            System.out.printf("     %3d%%       | %,10d ops | %,10d ops  |   %.2fx    |   +%.1f%%%n",
                sizePercent, baseline.totalOps, handshake.totalOps, speedup, improvement);
        }
        
        System.out.println("  " + "─".repeat(80));
        System.out.println("\n✅ Benchmark complete!");
        System.out.println("\nKey Insight: As size query frequency increases, the advantage of handshake");
        System.out.println("may change, showing the trade-off between update overhead and query efficiency.");
    }
}

