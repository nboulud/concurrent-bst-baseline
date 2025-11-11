package bench;

import bst.MyBST;
import bst.MyBSTBaseline;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Comprehensive benchmark for aggregate queries (size, rank, select)
 * Compares MyBST (handshake-based) with MyBSTBaseline (eager propagation)
 */
public class AggregateQueryBenchmark {

    interface BSTInterface {
        void insert(int k);
        void delete(int k);
        Integer get(int k);
        int size();
        int rank(int k);  // Returns rank of key k
        Integer select(int k);  // Returns kth smallest element
    }

    static class BaselineWrapper implements BSTInterface {
        private final MyBSTBaseline<Integer,Integer> map = new MyBSTBaseline<>();
        public void insert(int k) { map.put(k, k); }
        public void delete(int k) { map.remove(k); }
        public Integer get(int k) { return map.get(k); }
        public int size() { return map.sizeSnapshot(); }
        public int rank(int k) { return map.rank(k); }
        public Integer select(int k) { return map.select(k); }
    }

    static class HandshakeWrapper implements BSTInterface {
        private final MyBST<Integer,Integer> map = new MyBST<>();
        public void insert(int k) { map.put(k, k); }
        public void delete(int k) { map.remove(k); }
        public Integer get(int k) { return map.get(k); }
        public int size() { return map.sizeSnapshot(); }
        public int rank(int k) { return map.rank(k); }
        public Integer select(int k) { return map.select(k); }
    }

    static class TestConfig {
        int threads;
        int queryPercent;  // Percentage of aggregate queries (size/rank/select)
        String impl;
        
        TestConfig(int threads, int queryPercent, String impl) {
            this.threads = threads;
            this.queryPercent = queryPercent;
            this.impl = impl;
        }
    }
    
    static class Result {
        int threads;
        int queryPercent;
        long baselineOps;
        long handshakeOps;
        long baselineSizeOps;
        long handshakeSizeOps;
        long baselineRankOps;
        long handshakeRankOps;
        long baselineSelectOps;
        long handshakeSelectOps;
        
        double getSpeedup() {
            return (double)handshakeOps / baselineOps;
        }
        
        double getImprovement() {
            return 100.0 * (handshakeOps - baselineOps) / baselineOps;
        }
    }

    static class ThreadMetrics {
        long totalOps = 0;
        long sizeOps = 0;
        long rankOps = 0;
        long selectOps = 0;
    }

    static Result runTest(BSTInterface ds, int threads, int seconds, int queryPercent) throws Exception {
        // Preload with diverse keys
        for (int i = 0; i < 50_000; i++) {
            ds.insert(i);
        }

        ExecutorService pool = Executors.newFixedThreadPool(threads);
        CountDownLatch start = new CountDownLatch(1);
        CountDownLatch stop = new CountDownLatch(threads);

        final long endAt = System.nanoTime() + TimeUnit.SECONDS.toNanos(seconds);
        final ConcurrentLinkedQueue<ThreadMetrics> metricsQueue = new ConcurrentLinkedQueue<>();

        final int getPercent = 80 - queryPercent;  // Basic get operations
        final int insertPercent = 10;
        final int deletePercent = 10;
        
        // Split aggregate queries evenly
        final int sizePercent = queryPercent / 3;
        final int rankPercent = queryPercent / 3;
        final int selectPercent = queryPercent - sizePercent - rankPercent;

        for (int t = 0; t < threads; t++) {
            pool.submit(() -> {
                try {
                    Random rnd = new Random(Thread.currentThread().getId());
                    ThreadMetrics metrics = new ThreadMetrics();
                    start.await();
                    
                    while (System.nanoTime() < endAt) {
                        int k = rnd.nextInt(200_000);
                        int r = rnd.nextInt(100);
                        
                        if (r < getPercent) {
                            // Basic get operation
                            ds.get(k);
                        } else if (r < getPercent + insertPercent) {
                            // Insert operation
                            ds.insert(k);
                        } else if (r < getPercent + insertPercent + deletePercent) {
                            // Delete operation
                            ds.delete(k);
                        } else if (r < getPercent + insertPercent + deletePercent + sizePercent) {
                            // Size query
                            ds.size();
                            metrics.sizeOps++;
                        } else if (r < getPercent + insertPercent + deletePercent + sizePercent + rankPercent) {
                            // Rank query
                            ds.rank(k);
                            metrics.rankOps++;
                        } else {
                            // Select query (use valid range)
                            int pos = rnd.nextInt(50_000) + 1;  // 1-based
                            ds.select(pos);
                            metrics.selectOps++;
                        }
                        
                        metrics.totalOps++;
                    }
                    metricsQueue.add(metrics);
                } catch (InterruptedException ignored) { 
                } finally { 
                    stop.countDown(); 
                }
            });
        }

        start.countDown();
        stop.await();
        pool.shutdown();

        // Aggregate metrics
        Result result = new Result();
        for (ThreadMetrics m : metricsQueue) {
            result.baselineOps += m.totalOps;
            result.baselineSizeOps += m.sizeOps;
            result.baselineRankOps += m.rankOps;
            result.baselineSelectOps += m.selectOps;
        }

        return result;
    }

    public static void main(String[] args) throws Exception {
        int seconds = 10;
        int warmupSeconds = 5;
        int[] threadCounts = {16, 32, 64, 96};
        int[] queryPercentages = {5, 15, 30, 50};  // Percentage of aggregate queries
        
        System.out.println("╔═══════════════════════════════════════════════════════════════════════════╗");
        System.out.println("║              AGGREGATE QUERY PERFORMANCE BENCHMARK                        ║");
        System.out.println("║         (Size, Rank, Select Queries with Concurrent Updates)             ║");
        System.out.println("╚═══════════════════════════════════════════════════════════════════════════╝");
        System.out.println();
        System.out.println("Test Configuration:");
        System.out.println("  • Operations: get (varies), insert (10%), delete (10%), queries (varies)");
        System.out.println("  • Query mix: size, rank, select (equal distribution)");
        System.out.println("  • Preloaded size: 50,000 keys");
        System.out.println("  • Duration: " + seconds + " seconds per test");
        System.out.println();
        
        // Warmup phase
        System.out.println("═══════════════════════════════════════════════════════════════════════════");
        System.out.println("WARMUP PHASE: Triggering JIT compilation");
        System.out.println("═══════════════════════════════════════════════════════════════════════════");
        System.out.println();
        
        for (int i = 0; i < 3; i++) {
            System.out.printf("Warmup %d/3... ", i + 1);
            runTest(new BaselineWrapper(), 32, warmupSeconds, 30);
            runTest(new HandshakeWrapper(), 32, warmupSeconds, 30);
            System.out.println("done");
        }
        
        System.out.println("\n✓ JVM warmup complete\n");
        
        // Build test configurations
        List<TestConfig> allTests = new ArrayList<>();
        for (int threads : threadCounts) {
            for (int queryPercent : queryPercentages) {
                allTests.add(new TestConfig(threads, queryPercent, "baseline"));
                allTests.add(new TestConfig(threads, queryPercent, "handshake"));
            }
        }
        
        // Randomize test order
        Collections.shuffle(allTests, new Random(42));
        
        System.out.println("═══════════════════════════════════════════════════════════════════════════");
        System.out.println("RUNNING TESTS (randomized order)");
        System.out.println("═══════════════════════════════════════════════════════════════════════════");
        System.out.println();
        
        Map<String, Result> testResults = new HashMap<>();
        int testNum = 0;
        int totalTests = allTests.size();
        
        for (TestConfig config : allTests) {
            testNum++;
            BSTInterface ds = config.impl.equals("baseline") ? new BaselineWrapper() : new HandshakeWrapper();
            
            System.out.printf("[%2d/%2d] %10s: %2d threads, %2d%% queries... ", 
                testNum, totalTests, config.impl, config.threads, config.queryPercent);
            System.out.flush();
            
            Result result = runTest(ds, config.threads, seconds, config.queryPercent);
            String key = config.threads + "_" + config.queryPercent + "_" + config.impl;
            testResults.put(key, result);
            
            System.out.printf("%,10d ops/s%n", result.baselineOps / seconds);
        }
        
        // Aggregate results
        List<Result> results = new ArrayList<>();
        for (int threads : threadCounts) {
            for (int queryPercent : queryPercentages) {
                Result r = new Result();
                r.threads = threads;
                r.queryPercent = queryPercent;
                
                Result baseline = testResults.get(threads + "_" + queryPercent + "_baseline");
                Result handshake = testResults.get(threads + "_" + queryPercent + "_handshake");
                
                r.baselineOps = baseline.baselineOps;
                r.handshakeOps = handshake.baselineOps;
                r.baselineSizeOps = baseline.baselineSizeOps;
                r.handshakeSizeOps = handshake.baselineSizeOps;
                r.baselineRankOps = baseline.baselineRankOps;
                r.handshakeRankOps = handshake.baselineRankOps;
                r.baselineSelectOps = baseline.baselineSelectOps;
                r.handshakeSelectOps = handshake.baselineSelectOps;
                
                results.add(r);
            }
        }
        
        // Print results
        System.out.println("\n\n");
        System.out.println("╔═══════════════════════════════════════════════════════════════════════════╗");
        System.out.println("║                          THROUGHPUT RESULTS                               ║");
        System.out.println("╚═══════════════════════════════════════════════════════════════════════════╝");
        System.out.println();
        System.out.println("┌─────────┬──────────┬────────────────┬────────────────┬──────────┬───────────┐");
        System.out.println("│ Threads │ Query %  │  Baseline Ops  │ Handshake Ops  │ Speedup  │   Status  │");
        System.out.println("├─────────┼──────────┼────────────────┼────────────────┼──────────┼───────────┤");
        
        for (Result r : results) {
            String status = r.getSpeedup() > 1.05 ? "✓ FASTER" : 
                           r.getSpeedup() < 0.95 ? "✗ SLOWER" : "≈ SAME";
            System.out.printf("│  %3d    │   %2d%%   │  %,11d   │  %,11d   │  %5.2fx  │ %9s │%n",
                r.threads, r.queryPercent, r.baselineOps, r.handshakeOps, 
                r.getSpeedup(), status);
        }
        
        System.out.println("└─────────┴──────────┴────────────────┴────────────────┴──────────┴───────────┘");
        
        // Detailed query breakdown
        System.out.println("\n\n");
        System.out.println("╔═══════════════════════════════════════════════════════════════════════════╗");
        System.out.println("║                    QUERY TYPE BREAKDOWN (64 threads)                      ║");
        System.out.println("╚═══════════════════════════════════════════════════════════════════════════╝");
        System.out.println();
        System.out.println("┌──────────┬─────────────────┬─────────────────┬─────────────────┬──────────┐");
        System.out.println("│ Query %  │   Size Queries  │  Rank Queries   │ Select Queries  │ Speedup  │");
        System.out.println("├──────────┼─────────────────┼─────────────────┼─────────────────┼──────────┤");
        
        for (Result r : results) {
            if (r.threads == 64) {
                System.out.printf("│   %2d%%   │ %,7d / %,7d │ %,7d / %,7d │ %,7d / %,7d │  %5.2fx  │%n",
                    r.queryPercent,
                    r.baselineSizeOps, r.handshakeSizeOps,
                    r.baselineRankOps, r.handshakeRankOps,
                    r.baselineSelectOps, r.handshakeSelectOps,
                    r.getSpeedup());
            }
        }
        
        System.out.println("└──────────┴─────────────────┴─────────────────┴─────────────────┴──────────┘");
        System.out.println("Format: baseline / handshake");
        
        // Summary statistics
        System.out.println("\n\n");
        System.out.println("═══════════════════════════════════════════════════════════════════════════");
        System.out.println("                               SUMMARY");
        System.out.println("═══════════════════════════════════════════════════════════════════════════");
        System.out.println();
        
        double avgSpeedup = results.stream().mapToDouble(Result::getSpeedup).average().orElse(0.0);
        long wins = results.stream().filter(r -> r.getSpeedup() > 1.05).count();
        long losses = results.stream().filter(r -> r.getSpeedup() < 0.95).count();
        Result best = results.stream().max(Comparator.comparingDouble(Result::getSpeedup)).orElse(null);
        Result worst = results.stream().min(Comparator.comparingDouble(Result::getSpeedup)).orElse(null);
        
        System.out.printf("Average speedup:          %.2fx%n", avgSpeedup);
        System.out.printf("Handshake faster:         %d/%d (%.1f%%)%n", 
            wins, results.size(), 100.0 * wins / results.size());
        System.out.printf("Handshake slower:         %d/%d (%.1f%%)%n", 
            losses, results.size(), 100.0 * losses / results.size());
        
        if (best != null) {
            System.out.printf("Best case:                %.2fx (%d threads, %d%% queries)%n", 
                best.getSpeedup(), best.threads, best.queryPercent);
        }
        if (worst != null) {
            System.out.printf("Worst case:               %.2fx (%d threads, %d%% queries)%n", 
                worst.getSpeedup(), worst.threads, worst.queryPercent);
        }
        
        // High query percentage analysis
        List<Result> highQueryTests = results.stream()
            .filter(r -> r.queryPercent >= 30)
            .collect(java.util.stream.Collectors.toList());
        double highQueryAvg = highQueryTests.stream()
            .mapToDouble(Result::getSpeedup)
            .average().orElse(0.0);
        
        System.out.println();
        System.out.printf("High query workload (≥30%%): %.2fx average speedup%n", highQueryAvg);
        
        // Scalability analysis
        System.out.println();
        System.out.println("Scalability (30% queries):");
        for (int threads : threadCounts) {
            Result r = results.stream()
                .filter(res -> res.threads == threads && res.queryPercent == 30)
                .findFirst().orElse(null);
            if (r != null) {
                System.out.printf("  %3d threads: %.2fx speedup%n", threads, r.getSpeedup());
            }
        }
        
        System.out.println();
        System.out.println("═══════════════════════════════════════════════════════════════════════════");
        System.out.println();
        
        if (avgSpeedup > 1.05) {
            System.out.println("✅ CONCLUSION: Handshake algorithm significantly outperforms eager propagation");
            System.out.println("   The fast/slow path optimization provides substantial benefits for");
            System.out.println("   aggregate queries, especially under high concurrency.");
        } else if (avgSpeedup > 0.95) {
            System.out.println("⚖️  CONCLUSION: Handshake algorithm performance is comparable to eager propagation");
            System.out.println("   The added complexity of handshakes is offset by reduced propagation overhead.");
        } else {
            System.out.println("❌ CONCLUSION: Handshake algorithm underperforms compared to eager propagation");
            System.out.println("   Handshake overhead may exceed benefits in current workload.");
        }
        
        System.out.println();
        System.out.println("Key Insights:");
        System.out.println("  • Rank/select queries leverage fastSize metadata in tree nodes");
        System.out.println("  • Size queries are O(1) with fast path optimization");
        System.out.println("  • Handshake overhead amortized across many updates");
        
        System.out.println("\n✅ Aggregate query benchmark complete!");
    }
}
