package bench;

import bst.MyBSTnext;
import bst.MyBSTBaseline;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Fair comparison with proper JVM warmup and randomized test order
 * MODIFIED: Using MyBSTv2 (per-thread reader state optimization)
 */
public class FairComparison {

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

    static class TestConfig {
        int threads;
        int sizePercent;
        String impl;  // "baseline" or "handshake"
        
        TestConfig(int threads, int sizePercent, String impl) {
            this.threads = threads;
            this.sizePercent = sizePercent;
            this.impl = impl;
        }
    }
    
    static class Result {
        int threads;
        int sizePercent;
        long baselineOps;
        long handshakeOps;
        
        double getSpeedup() {
            return (double)handshakeOps / baselineOps;
        }
        
        double getImprovement() {
            return 100.0 * (handshakeOps - baselineOps) / baselineOps;
        }
    }

    static long runTest(BSTInterface ds, int threads, int seconds, int sizePercent) throws Exception {
        // Preload
        for (int i = 0; i < 50_000; i++) ds.insert(i);

        ExecutorService pool = Executors.newFixedThreadPool(threads);
        CountDownLatch start = new CountDownLatch(1);
        CountDownLatch stop = new CountDownLatch(threads);

        final long endAt = System.nanoTime() + TimeUnit.SECONDS.toNanos(seconds);
        final ConcurrentLinkedQueue<Long> counts = new ConcurrentLinkedQueue<>();

        final int getPercent = 80 - sizePercent;

        for (int t = 0; t < threads; t++) {
            pool.submit(() -> {
                try {
                    Random rnd = new Random(Thread.currentThread().getId());
                    long ops = 0;
                    start.await();
                    while (System.nanoTime() < endAt) {
                        int k = rnd.nextInt(200_000);
                        int r = rnd.nextInt(100);
                        
                        if (r < getPercent) ds.get(k);
                        else if (r < getPercent + 10) ds.insert(k);
                        else if (r < getPercent + 20) ds.delete(k);
                        else ds.size();
                        
                        ops++;
                    }
                    counts.add(ops);
                } catch (InterruptedException ignored) { }
                finally { stop.countDown(); }
            });
        }

        start.countDown();
        stop.await();
        pool.shutdown();

        return counts.stream().mapToLong(Long::longValue).sum();
    }

    public static void main(String[] args) throws Exception {
        int seconds = 10;
        int warmupSeconds = 5;
        int[] threadCounts = {16, 32, 64, 96};
        int[] sizePercentages = {5, 15, 30};
        
        System.out.println("╔════════════════════════════════════════════════════════════════════════════╗");
        System.out.println("║                     FAIR PERFORMANCE COMPARISON                            ║");
        System.out.println("║              (With JVM Warmup and Randomized Test Order)                  ║");
        System.out.println("╚════════════════════════════════════════════════════════════════════════════╝");
        System.out.println();
        System.out.println("Methodology:");
        System.out.println("  • Warmup runs before measurement");
        System.out.println("  • Randomized test order to eliminate bias");
        System.out.println("  • " + seconds + " seconds per test");
        System.out.println();
        
        // Warmup phase
        System.out.println("═══════════════════════════════════════════════════════════════════════════");
        System.out.println("WARMUP PHASE: Running both implementations to trigger JIT compilation");
        System.out.println("═══════════════════════════════════════════════════════════════════════════");
        System.out.println();
        
        for (int i = 0; i < 3; i++) {
            System.out.printf("Warmup %d/3... ", i + 1);
            runTest(new BaselineWrapper(), 96, warmupSeconds, 15);
            runTest(new HandshakeWrapper(), 96, warmupSeconds, 15);
            System.out.println("done");
        }
        
        System.out.println("\n✓ JVM warmup complete\n");
        
        // Build list of all test configurations
        List<TestConfig> allTests = new ArrayList<>();
        for (int threads : threadCounts) {
            for (int sizePercent : sizePercentages) {
                allTests.add(new TestConfig(threads, sizePercent, "baseline"));
                allTests.add(new TestConfig(threads, sizePercent, "handshake"));
            }
        }
        
        // Randomize test order
        Collections.shuffle(allTests, new Random(42));  // Fixed seed for reproducibility
        
        System.out.println("═══════════════════════════════════════════════════════════════════════════");
        System.out.println("RUNNING TESTS (randomized order)");
        System.out.println("═══════════════════════════════════════════════════════════════════════════");
        System.out.println();
        
        Map<String, Long> testResults = new HashMap<>();
        int testNum = 0;
        int totalTests = allTests.size();
        
        for (TestConfig config : allTests) {
            testNum++;
            BSTInterface ds = config.impl.equals("baseline") ? new BaselineWrapper() : new HandshakeWrapper();
            
            System.out.printf("[%2d/%2d] %s: %d threads, %d%% size... ", 
                testNum, totalTests, config.impl, config.threads, config.sizePercent);
            System.out.flush();
            
            long ops = runTest(ds, config.threads, seconds, config.sizePercent);
            String key = config.threads + "_" + config.sizePercent + "_" + config.impl;
            testResults.put(key, ops);
            
            System.out.printf("%,d ops%n", ops);
        }
        
        // Aggregate results
        List<Result> results = new ArrayList<>();
        for (int threads : threadCounts) {
            for (int sizePercent : sizePercentages) {
                Result r = new Result();
                r.threads = threads;
                r.sizePercent = sizePercent;
                r.baselineOps = testResults.get(threads + "_" + sizePercent + "_baseline");
                r.handshakeOps = testResults.get(threads + "_" + sizePercent + "_handshake");
                results.add(r);
            }
        }
        
        // Print results
        System.out.println("\n\n");
        System.out.println("╔════════════════════════════════════════════════════════════════════════════╗");
        System.out.println("║                          FINAL RESULTS                                     ║");
        System.out.println("╚════════════════════════════════════════════════════════════════════════════╝");
        System.out.println();
        System.out.println("┌─────────┬──────────┬───────────────┬───────────────┬──────────┬────────────┐");
        System.out.println("│ Threads │ Size %   │   Baseline    │   Handshake   │ Speedup  │ Improvement│");
        System.out.println("├─────────┼──────────┼───────────────┼───────────────┼──────────┼────────────┤");
        
        for (Result r : results) {
            String indicator = r.getSpeedup() > 1.0 ? "✓" : " ";
            System.out.printf("│  %3d    │   %2d%%   │  %,10d   │  %,10d   │  %5.2fx %s │   %+6.1f%%  │%n",
                r.threads, r.sizePercent, r.baselineOps, r.handshakeOps, 
                r.getSpeedup(), indicator, r.getImprovement());
        }
        
        System.out.println("└─────────┴──────────┴───────────────┴───────────────┴──────────┴────────────┘");
        
        // Summary statistics
        double avgSpeedup = results.stream().mapToDouble(Result::getSpeedup).average().orElse(0.0);
        long wins = results.stream().filter(r -> r.getSpeedup() > 1.0).count();
        Result best = results.stream().max(Comparator.comparingDouble(Result::getSpeedup)).orElse(null);
        Result worst = results.stream().min(Comparator.comparingDouble(Result::getSpeedup)).orElse(null);
        
        System.out.println();
        System.out.println("═══════════════════════════════════════════════════════════════════════════");
        System.out.println("                            SUMMARY");
        System.out.println("═══════════════════════════════════════════════════════════════════════════");
        System.out.println();
        System.out.printf("Average speedup:        %.2fx%n", avgSpeedup);
        System.out.printf("Handshake wins:         %d/%d (%.1f%%)%n", wins, results.size(), 100.0 * wins / results.size());
        if (best != null) {
            System.out.printf("Best case:              %.2fx (%d threads, %d%% size)%n", 
                best.getSpeedup(), best.threads, best.sizePercent);
        }
        if (worst != null) {
            System.out.printf("Worst case:             %.2fx (%d threads, %d%% size)%n", 
                worst.getSpeedup(), worst.threads, worst.sizePercent);
        }
        
        System.out.println();
        System.out.println("═══════════════════════════════════════════════════════════════════════════");
        System.out.println();
        
        if (avgSpeedup > 1.0) {
            System.out.println("✅ CONCLUSION: Handshake algorithm outperforms eager propagation on average");
        } else {
            System.out.println("❌ CONCLUSION: Handshake algorithm does not outperform eager propagation");
        }
        
        System.out.println("\n✅ Fair comparison complete!");
    }
}

