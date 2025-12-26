package bench;

import bst.MyBSTnext;
import bst.MyBSTBaseline;
import java.util.*;
import java.util.concurrent.*;

/**
 * Focused benchmark comparing operation types separately
 * Tests each operation type (insert, size, rank, select) independently
 */
public class OperationBenchmark {

    interface BSTInterface {
        void insert(int k);
        void delete(int k);
        Integer get(int k);
        int size();
        int rank(int k);
        Integer select(int k);
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
        private final MyBSTnext<Integer,Integer> map = new MyBSTnext<>();
        public void insert(int k) { map.put(k, k); }
        public void delete(int k) { map.remove(k); }
        public Integer get(int k) { return map.get(k); }
        public int size() { return map.sizeSnapshot(); }
        public int rank(int k) { return map.rank(k); }
        public Integer select(int k) { return map.select(k); }
    }

    enum OpType {
        INSERT, SIZE, RANK, SELECT
    }

    static class Result {
        String impl;
        int threads;
        OpType opType;
        long opsPerSecond;
        
        double getSpeedup(Result baseline) {
            return (double)opsPerSecond / baseline.opsPerSecond;
        }
    }

    static class WorkloadConfig {
        int sizeThreads;
        int rankThreads;
        int selectThreads;
        int insertThreads;
        int deleteThreads;
        int getThreads;
        
        int totalThreads() {
            return sizeThreads + rankThreads + selectThreads + insertThreads + deleteThreads + getThreads;
        }
        
        @Override
        public String toString() {
            return String.format("size:%d, rank:%d, select:%d, insert:%d, delete:%d, get:%d",
                sizeThreads, rankThreads, selectThreads, insertThreads, deleteThreads, getThreads);
        }
    }
    
    static class DetailedResult {
        String impl;
        WorkloadConfig config;
        long totalOps;
        long sizeOps;
        long rankOps;
        long selectOps;
        long insertOps;
        long deleteOps;
        long getOps;
    }

    static DetailedResult runTest(BSTInterface ds, WorkloadConfig config, int seconds) throws Exception {
        // Preload with diverse keys
        for (int i = 0; i < 50_000; i++) {
            ds.insert(i);
        }

        ExecutorService pool = Executors.newFixedThreadPool(config.totalThreads());
        CountDownLatch start = new CountDownLatch(1);
        CountDownLatch stop = new CountDownLatch(config.totalThreads());

        final long endAt = System.nanoTime() + TimeUnit.SECONDS.toNanos(seconds);
        final ConcurrentLinkedQueue<Long> allCounts = new ConcurrentLinkedQueue<>();
        final ConcurrentLinkedQueue<Long> sizeCounts = new ConcurrentLinkedQueue<>();
        final ConcurrentLinkedQueue<Long> rankCounts = new ConcurrentLinkedQueue<>();
        final ConcurrentLinkedQueue<Long> selectCounts = new ConcurrentLinkedQueue<>();
        final ConcurrentLinkedQueue<Long> insertCounts = new ConcurrentLinkedQueue<>();
        final ConcurrentLinkedQueue<Long> deleteCounts = new ConcurrentLinkedQueue<>();
        final ConcurrentLinkedQueue<Long> getCounts = new ConcurrentLinkedQueue<>();

        // Create threads for each operation type
        
        // Size threads
        for (int t = 0; t < config.sizeThreads; t++) {
            pool.submit(() -> {
                try {
                    long ops = 0;
                    start.await();
                    while (System.nanoTime() < endAt) {
                        ds.size();
                        ops++;
                    }
                    sizeCounts.add(ops);
                    allCounts.add(ops);
                } catch (InterruptedException ignored) { 
                } finally { stop.countDown(); }
            });
        }
        
        // Rank threads
        for (int t = 0; t < config.rankThreads; t++) {
            pool.submit(() -> {
                try {
                    Random rnd = new Random(Thread.currentThread().getId());
                    long ops = 0;
                    start.await();
                    while (System.nanoTime() < endAt) {
                        int k = rnd.nextInt(200_000);
                        ds.rank(k);
                        ops++;
                    }
                    rankCounts.add(ops);
                    allCounts.add(ops);
                } catch (InterruptedException ignored) { 
                } finally { stop.countDown(); }
            });
        }
        
        // Select threads
        for (int t = 0; t < config.selectThreads; t++) {
            pool.submit(() -> {
                try {
                    Random rnd = new Random(Thread.currentThread().getId());
                    long ops = 0;
                    start.await();
                    while (System.nanoTime() < endAt) {
                        int pos = rnd.nextInt(50_000) + 1;
                        ds.select(pos);
                        ops++;
                    }
                    selectCounts.add(ops);
                    allCounts.add(ops);
                } catch (InterruptedException ignored) { 
                } finally { stop.countDown(); }
            });
        }
        
        // Insert threads
        for (int t = 0; t < config.insertThreads; t++) {
            pool.submit(() -> {
                try {
                    Random rnd = new Random(Thread.currentThread().getId());
                    long ops = 0;
                    start.await();
                    while (System.nanoTime() < endAt) {
                        int k = rnd.nextInt(200_000);
                        ds.insert(k);
                        ops++;
                    }
                    insertCounts.add(ops);
                    allCounts.add(ops);
                } catch (InterruptedException ignored) { 
                } finally { stop.countDown(); }
            });
        }
        
        // Delete threads
        for (int t = 0; t < config.deleteThreads; t++) {
            pool.submit(() -> {
                try {
                    Random rnd = new Random(Thread.currentThread().getId());
                    long ops = 0;
                    start.await();
                    while (System.nanoTime() < endAt) {
                        int k = rnd.nextInt(200_000);
                        ds.delete(k);
                        ops++;
                    }
                    deleteCounts.add(ops);
                    allCounts.add(ops);
                } catch (InterruptedException ignored) { 
                } finally { stop.countDown(); }
            });
        }
        
        // Get threads
        for (int t = 0; t < config.getThreads; t++) {
            pool.submit(() -> {
                try {
                    Random rnd = new Random(Thread.currentThread().getId());
                    long ops = 0;
                    start.await();
                    while (System.nanoTime() < endAt) {
                        int k = rnd.nextInt(200_000);
                        ds.get(k);
                        ops++;
                    }
                    getCounts.add(ops);
                    allCounts.add(ops);
                } catch (InterruptedException ignored) { 
                } finally { stop.countDown(); }
            });
        }

        start.countDown();
        stop.await();
        pool.shutdown();

        DetailedResult result = new DetailedResult();
        result.config = config;
        result.totalOps = allCounts.stream().mapToLong(Long::longValue).sum();
        result.sizeOps = sizeCounts.stream().mapToLong(Long::longValue).sum();
        result.rankOps = rankCounts.stream().mapToLong(Long::longValue).sum();
        result.selectOps = selectCounts.stream().mapToLong(Long::longValue).sum();
        result.insertOps = insertCounts.stream().mapToLong(Long::longValue).sum();
        result.deleteOps = deleteCounts.stream().mapToLong(Long::longValue).sum();
        result.getOps = getCounts.stream().mapToLong(Long::longValue).sum();
        
        return result;
    }

    public static void main(String[] args) throws Exception {
        int seconds = 10;
        int warmupSeconds = 3;
        
        // Define workload configurations
        // Format: size, rank, select, insert, delete, get threads
        List<WorkloadConfig> workloads = new ArrayList<>();
        
        // Workload 1: Heavy query load (30% queries)
        WorkloadConfig w1 = new WorkloadConfig();
        w1.sizeThreads = 10;
        w1.rankThreads = 10;
        w1.selectThreads = 10;
        w1.insertThreads = 10;
        w1.deleteThreads = 10;
        w1.getThreads = 50;
        workloads.add(w1);
        
        // Workload 2: Balanced (20% queries)
        WorkloadConfig w2 = new WorkloadConfig();
        w2.sizeThreads = 6;
        w2.rankThreads = 7;
        w2.selectThreads = 7;
        w2.insertThreads = 10;
        w2.deleteThreads = 10;
        w2.getThreads = 60;
        workloads.add(w2);
        
        // Workload 3: Light queries (10% queries)
        WorkloadConfig w3 = new WorkloadConfig();
        w3.sizeThreads = 3;
        w3.rankThreads = 3;
        w3.selectThreads = 4;
        w3.insertThreads = 10;
        w3.deleteThreads = 10;
        w3.getThreads = 70;
        workloads.add(w3);
        
        // Workload 4: Query-heavy (50% queries)
        WorkloadConfig w4 = new WorkloadConfig();
        w4.sizeThreads = 16;
        w4.rankThreads = 17;
        w4.selectThreads = 17;
        w4.insertThreads = 10;
        w4.deleteThreads = 10;
        w4.getThreads = 26;
        workloads.add(w4);
        
        System.out.println("╔═══════════════════════════════════════════════════════════════════════════╗");
        System.out.println("║              OPERATION-SPECIFIC PERFORMANCE BENCHMARK                     ║");
        System.out.println("║        (Dedicated threads for each operation type per workload)          ║");
        System.out.println("╚═══════════════════════════════════════════════════════════════════════════╝");
        System.out.println();
        System.out.println("Configuration:");
        System.out.println("  • Each thread performs ONLY ONE operation type");
        System.out.println("  • Preloaded size: 50,000 keys");
        System.out.println("  • Duration: " + seconds + " seconds per test");
        System.out.println();
        System.out.println("Workload Configurations:");
        for (int i = 0; i < workloads.size(); i++) {
            WorkloadConfig w = workloads.get(i);
            System.out.printf("  Workload %d (%d threads): %s%n", i+1, w.totalThreads(), w);
        }
        System.out.println();
        
        // Warmup
        System.out.println("═══════════════════════════════════════════════════════════════════════════");
        System.out.println("WARMUP PHASE");
        System.out.println("═══════════════════════════════════════════════════════════════════════════");
        for (int i = 0; i < 2; i++) {
            System.out.printf("Warmup %d/2... ", i + 1);
            runTest(new BaselineWrapper(), workloads.get(0), warmupSeconds);
            runTest(new HandshakeWrapper(), workloads.get(0), warmupSeconds);
            System.out.println("done");
        }
        System.out.println("\n✓ Warmup complete\n");
        
        System.out.println("═══════════════════════════════════════════════════════════════════════════");
        System.out.println("RUNNING TESTS");
        System.out.println("═══════════════════════════════════════════════════════════════════════════");
        System.out.println();
        
        // Store results: [workloadIndex] -> {baseline, handshake} DetailedResult
        List<DetailedResult> baselineResults = new ArrayList<>();
        List<DetailedResult> handshakeResults = new ArrayList<>();
        
        // Run tests for each workload
        for (int i = 0; i < workloads.size(); i++) {
            WorkloadConfig config = workloads.get(i);
            System.out.println("─────────────────────────────────────────────────────────────────────────");
            System.out.printf("Workload %d (%d threads): %s%n", i+1, config.totalThreads(), config);
            System.out.println("─────────────────────────────────────────────────────────────────────────");
            
            // Test baseline
            BSTInterface ds = new BaselineWrapper();
            System.out.print("  MyBSTBaseline (eager propagation)... ");
            DetailedResult baselineResult = runTest(ds, config, seconds);
            baselineResults.add(baselineResult);
            System.out.printf("✓ %,d total ops/sec%n", baselineResult.totalOps / seconds);
            System.out.printf("    Size: %,d | Rank: %,d | Select: %,d | Insert: %,d | Delete: %,d | Get: %,d%n",
                baselineResult.sizeOps / seconds, baselineResult.rankOps / seconds, 
                baselineResult.selectOps / seconds, baselineResult.insertOps / seconds,
                baselineResult.deleteOps / seconds, baselineResult.getOps / seconds);
            
            // Test handshake
            ds = new HandshakeWrapper();
            System.out.print("  MyBST (handshake + fast path)... ");
            DetailedResult handshakeResult = runTest(ds, config, seconds);
            handshakeResults.add(handshakeResult);
            
            // Calculate speedup
            double overallSpeedup = (double) handshakeResult.totalOps / baselineResult.totalOps;
            System.out.printf("✓ %,d total ops/sec (%.2fx speedup)%n", 
                handshakeResult.totalOps / seconds, overallSpeedup);
            System.out.printf("    Size: %,d | Rank: %,d | Select: %,d | Insert: %,d | Delete: %,d | Get: %,d%n",
                handshakeResult.sizeOps / seconds, handshakeResult.rankOps / seconds, 
                handshakeResult.selectOps / seconds, handshakeResult.insertOps / seconds,
                handshakeResult.deleteOps / seconds, handshakeResult.getOps / seconds);
            System.out.println();
        }
        
        // Summary by workload
        System.out.println("═══════════════════════════════════════════════════════════════════════════");
        System.out.println("SUMMARY: Speedup Analysis");
        System.out.println("═══════════════════════════════════════════════════════════════════════════");
        System.out.printf("%-15s | %-10s | %-10s | %-10s | %-10s | %-10s | %-10s | %-10s%n",
            "Workload", "Overall", "Size", "Rank", "Select", "Insert", "Delete", "Get");
        System.out.println("───────────────────────────────────────────────────────────────────────────");
        
        for (int i = 0; i < workloads.size(); i++) {
            DetailedResult baseline = baselineResults.get(i);
            DetailedResult handshake = handshakeResults.get(i);
            
            double overallSpeedup = (double) handshake.totalOps / baseline.totalOps;
            double sizeSpeedup = baseline.sizeOps > 0 ? (double) handshake.sizeOps / baseline.sizeOps : 1.0;
            double rankSpeedup = baseline.rankOps > 0 ? (double) handshake.rankOps / baseline.rankOps : 1.0;
            double selectSpeedup = baseline.selectOps > 0 ? (double) handshake.selectOps / baseline.selectOps : 1.0;
            double insertSpeedup = baseline.insertOps > 0 ? (double) handshake.insertOps / baseline.insertOps : 1.0;
            double deleteSpeedup = baseline.deleteOps > 0 ? (double) handshake.deleteOps / baseline.deleteOps : 1.0;
            double getSpeedup = baseline.getOps > 0 ? (double) handshake.getOps / baseline.getOps : 1.0;
            
            System.out.printf("Workload %-6d | %.2fx      | %.2fx     | %.2fx     | %.2fx       | %.2fx      | %.2fx      | %.2fx%n",
                i+1, overallSpeedup, sizeSpeedup, rankSpeedup, selectSpeedup, 
                insertSpeedup, deleteSpeedup, getSpeedup);
        }
        System.out.println();
        
        // Key insights
        System.out.println("═══════════════════════════════════════════════════════════════════════════");
        System.out.println("KEY INSIGHTS:");
        
        // Compute average speedup per operation type across all workloads
        double avgSizeSpeedup = 0, avgRankSpeedup = 0, avgSelectSpeedup = 0;
        double avgInsertSpeedup = 0, avgDeleteSpeedup = 0, avgGetSpeedup = 0;
        
        for (int i = 0; i < workloads.size(); i++) {
            DetailedResult baseline = baselineResults.get(i);
            DetailedResult handshake = handshakeResults.get(i);
            
            if (baseline.sizeOps > 0) avgSizeSpeedup += (double) handshake.sizeOps / baseline.sizeOps;
            if (baseline.rankOps > 0) avgRankSpeedup += (double) handshake.rankOps / baseline.rankOps;
            if (baseline.selectOps > 0) avgSelectSpeedup += (double) handshake.selectOps / baseline.selectOps;
            if (baseline.insertOps > 0) avgInsertSpeedup += (double) handshake.insertOps / baseline.insertOps;
            if (baseline.deleteOps > 0) avgDeleteSpeedup += (double) handshake.deleteOps / baseline.deleteOps;
            if (baseline.getOps > 0) avgGetSpeedup += (double) handshake.getOps / baseline.getOps;
        }
        
        int n = workloads.size();
        avgSizeSpeedup /= n; avgRankSpeedup /= n; avgSelectSpeedup /= n;
        avgInsertSpeedup /= n; avgDeleteSpeedup /= n; avgGetSpeedup /= n;
        
        System.out.printf("  • Size:   %.2fx average speedup - %s%n", avgSizeSpeedup, getVerdict(avgSizeSpeedup));
        System.out.printf("  • Rank:   %.2fx average speedup - %s%n", avgRankSpeedup, getVerdict(avgRankSpeedup));
        System.out.printf("  • Select: %.2fx average speedup - %s%n", avgSelectSpeedup, getVerdict(avgSelectSpeedup));
        System.out.printf("  • Insert: %.2fx average speedup - %s%n", avgInsertSpeedup, getVerdict(avgInsertSpeedup));
        System.out.printf("  • Delete: %.2fx average speedup - %s%n", avgDeleteSpeedup, getVerdict(avgDeleteSpeedup));
        System.out.printf("  • Get:    %.2fx average speedup - %s%n", avgGetSpeedup, getVerdict(avgGetSpeedup));
        System.out.println("═══════════════════════════════════════════════════════════════════════════");
        
        System.out.println("\n✅ Mixed workload benchmark complete!");
    }
    
    private static String getVerdict(double speedup) {
        if (speedup > 1.5) return "SIGNIFICANT IMPROVEMENT";
        if (speedup > 1.2) return "GOOD IMPROVEMENT";
        if (speedup > 1.0) return "MODERATE IMPROVEMENT";
        if (speedup > 0.95) return "ROUGHLY EQUAL";
        return "SLOWER";
    }
}
