package bench;

import bst.MyBST;
import java.util.Random;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Debug benchmark to understand handshake performance characteristics
 */
public class DebugHandshake {

    static class HandshakeBSTKV {
        private final MyBST<Integer,Integer> map = new MyBST<>();
        public void insert(int k) { map.put(k, k); }
        public void delete(int k) { map.remove(k); }
        public Integer get(int k) { return map.get(k); }
        public int size() { return map.sizeSnapshot(); }
        public String getProfilingStats() { return map.getProfilingStats(); }
    }

    public static void main(String[] args) throws Exception {
        int threads = (args.length >= 1) ? Integer.parseInt(args[0]) : 16;
        int seconds = (args.length >= 2) ? Integer.parseInt(args[1]) : 10;
        int sizePercent = (args.length >= 3) ? Integer.parseInt(args[2]) : 5;

        System.out.println("================================================================================");
        System.out.println("              DEBUG HANDSHAKE PERFORMANCE");
        System.out.println("================================================================================");
        System.out.printf("Configuration: %d threads, %d seconds, %d%% size queries%n", threads, seconds, sizePercent);
        System.out.println("--------------------------------------------------------------------------------\n");

        HandshakeBSTKV ds = new HandshakeBSTKV();

        // Preload
        System.out.println("Preloading 50k elements...");
        long preloadStart = System.nanoTime();
        for (int i = 0; i < 50_000; i++) ds.insert(i);
        long preloadTime = (System.nanoTime() - preloadStart) / 1_000_000;
        System.out.printf("Preload complete in %d ms%n", preloadTime);
        
        int initialSize = ds.size();
        System.out.printf("Initial size: %d%n", initialSize);
        System.out.printf("After preload: %s%n%n", ds.getProfilingStats());

        ExecutorService pool = Executors.newFixedThreadPool(threads);
        CountDownLatch start = new CountDownLatch(1);
        CountDownLatch stop = new CountDownLatch(threads);

        final long endAt = System.nanoTime() + TimeUnit.SECONDS.toNanos(seconds);
        final ConcurrentLinkedQueue<Long> counts = new ConcurrentLinkedQueue<>();
        final ConcurrentLinkedQueue<Long> sizeQueries = new ConcurrentLinkedQueue<>();
        final ConcurrentLinkedQueue<Long> insertCounts = new ConcurrentLinkedQueue<>();
        final ConcurrentLinkedQueue<Long> deleteCounts = new ConcurrentLinkedQueue<>();
        final ConcurrentLinkedQueue<Long> getCounts = new ConcurrentLinkedQueue<>();
        final AtomicLong sizeErrorCount = new AtomicLong(0);

        final int getPercent = 80 - sizePercent;
        final int insertPercent = 10;
        final int deletePercent = 10;

        for (int t = 0; t < threads; t++) {
            pool.submit(() -> {
                try {
                    Random rnd = new Random(Thread.currentThread().getId());
                    long ops = 0, sizeOps = 0, inserts = 0, deletes = 0, gets = 0;
                    start.await();
                    while (System.nanoTime() < endAt) {
                        int k = rnd.nextInt(200_000);
                        int r = rnd.nextInt(100);
                        
                        if (r < getPercent) { 
                            ds.get(k); 
                            gets++;
                        }
                        else if (r < getPercent + insertPercent) { 
                            ds.insert(k);
                            inserts++;
                        }
                        else if (r < getPercent + insertPercent + deletePercent) { 
                            ds.delete(k);
                            deletes++;
                        }
                        else { 
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
                    insertCounts.add(inserts);
                    deleteCounts.add(deletes);
                    getCounts.add(gets);
                } catch (InterruptedException ignored) { }
                finally { stop.countDown(); }
            });
        }

        System.out.println("Starting benchmark...\n");
        long benchStart = System.nanoTime();
        start.countDown();
        stop.await();
        pool.shutdown();
        long benchTime = (System.nanoTime() - benchStart) / 1_000_000;

        long totalOps = counts.stream().mapToLong(Long::longValue).sum();
        long totalSizeQueries = sizeQueries.stream().mapToLong(Long::longValue).sum();
        long totalInserts = insertCounts.stream().mapToLong(Long::longValue).sum();
        long totalDeletes = deleteCounts.stream().mapToLong(Long::longValue).sum();
        long totalGets = getCounts.stream().mapToLong(Long::longValue).sum();
        
        int finalSize = ds.size();

        System.out.println("================================================================================");
        System.out.println("                           RESULTS");
        System.out.println("================================================================================");
        System.out.printf("Total operations:    %,10d (%.2f Mops/s)%n", totalOps, totalOps / (double)seconds / 1_000_000.0);
        System.out.printf("  - Gets:            %,10d (%.1f%%)%n", totalGets, 100.0 * totalGets / totalOps);
        System.out.printf("  - Inserts:         %,10d (%.1f%%)%n", totalInserts, 100.0 * totalInserts / totalOps);
        System.out.printf("  - Deletes:         %,10d (%.1f%%)%n", totalDeletes, 100.0 * totalDeletes / totalOps);
        System.out.printf("  - Size queries:    %,10d (%.1f%%)%n", totalSizeQueries, 100.0 * totalSizeQueries / totalOps);
        System.out.println();
        System.out.printf("Final size: %d%n", finalSize);
        System.out.printf("Size errors: %d%n", sizeErrorCount.get());
        System.out.printf("Benchmark time: %d ms%n", benchTime);
        System.out.println();
        
        // Profiling information
        System.out.println("================================================================================");
        System.out.println("                      HANDSHAKE PROFILING");
        System.out.println("================================================================================");
        System.out.println(ds.getProfilingStats());
        System.out.println();
        
        long handshakes = ds.map.totalHandshakes.get();
        long handshakeTimeNanos = ds.map.totalHandshakeTimeNanos.get();
        long sizeCalls = ds.map.totalSizeCalls.get();
        
        if (handshakes > 0) {
            double avgHandshakeUs = handshakeTimeNanos / (double)handshakes / 1000.0;
            double totalHandshakeMs = handshakeTimeNanos / 1_000_000.0;
            double handshakeOverheadPercent = 100.0 * totalHandshakeMs / benchTime;
            
            System.out.printf("Total handshake time:     %.2f ms (%.1f%% of benchmark time)%n", 
                totalHandshakeMs, handshakeOverheadPercent);
            System.out.printf("Average handshake time:   %.2f μs%n", avgHandshakeUs);
            System.out.printf("Handshakes per size:      %.1f (should be ~2.0)%n", handshakes / (double)sizeCalls);
            System.out.printf("Size operations per sec:  %.1f%n", sizeCalls * 1000.0 / benchTime);
            System.out.println();
            
            if (avgHandshakeUs > 1000) {
                System.out.println("⚠️  WARNING: Handshakes are taking > 1ms each!");
                System.out.println("   This is very slow and indicates a performance problem.");
            } else if (avgHandshakeUs > 100) {
                System.out.println("⚠️  Handshakes taking > 100μs - this may be too slow.");
            } else {
                System.out.println("✓ Handshake timing seems reasonable.");
            }
        }
        
        System.out.println("\n✅ Debug benchmark complete!");
    }
}

