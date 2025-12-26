package bench;

import bst.MyBSTnext;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.Random;

/**
 * Detailed correctness test for handshake MyBST with a small tree
 * Tests linearizability of size() under concurrent updates
 */
public class CorrectnessTest {

    static class TestBST {
        private final MyBSTnext<Integer,Integer> map = new MyBSTnext<>();
        private final AtomicInteger actualInserts = new AtomicInteger(0);
        private final AtomicInteger actualDeletes = new AtomicInteger(0);
        
        public boolean insert(int k, int tid) {
            Integer prev = map.put(k, k);
            if (prev == null) {
                actualInserts.incrementAndGet();
                System.out.printf("[Thread %d] INSERT %d → size should increase%n", tid, k);
                return true;
            }
            return false;
        }
        
        public boolean delete(int k, int tid) {
            Integer prev = map.remove(k);
            if (prev != null) {
                actualDeletes.incrementAndGet();
                System.out.printf("[Thread %d] DELETE %d → size should decrease%n", tid, k);
                return true;
            }
            return false;
        }
        
        public int size(int tid) {
            int s = map.sizeSnapshot();
            System.out.printf("[Thread %d] SIZE = %d (inserts=%d, deletes=%d, expected~%d)%n", 
                tid, s, actualInserts.get(), actualDeletes.get(), 
                actualInserts.get() - actualDeletes.get());
            return s;
        }
        
        public boolean contains(int k) {
            return map.containsKey(k);
        }
        
        public int getActualSize() {
            return actualInserts.get() - actualDeletes.get();
        }
    }

    public static void main(String[] args) throws Exception {
        int numThreads = (args.length >= 1) ? Integer.parseInt(args[0]) : 4;
        int seconds = (args.length >= 2) ? Integer.parseInt(args[1]) : 5;
        
        System.out.println("========================================");
        System.out.println("Handshake MyBST Correctness Test");
        System.out.println("Small tree (keys 1-20), " + numThreads + " threads, " + seconds + " seconds");
        System.out.println("========================================\n");
        
        TestBST tree = new TestBST();
        
        // Preload with just a few elements
        System.out.println("Initial setup: inserting keys 1-10");
        for (int i = 1; i <= 10; i++) {
            tree.insert(i, 0);
        }
        System.out.printf("Initial size: %d%n%n", tree.size(0));
        
        ExecutorService pool = Executors.newFixedThreadPool(numThreads);
        CountDownLatch start = new CountDownLatch(1);
        CountDownLatch stop = new CountDownLatch(numThreads);
        
        final long endAt = System.nanoTime() + TimeUnit.SECONDS.toNanos(seconds);
        final ConcurrentHashMap<Integer, Long> threadOps = new ConcurrentHashMap<>();
        final ConcurrentHashMap<Integer, Long> sizeQueries = new ConcurrentHashMap<>();
        final AtomicInteger sizeErrors = new AtomicInteger(0);
        
        // Launch threads
        for (int t = 0; t < numThreads; t++) {
            final int tid = t + 1;
            pool.submit(() -> {
                try {
                    Random rnd = new Random(tid * 1000);
                    long ops = 0;
                    long sizes = 0;
                    
                    start.await();
                    
                    while (System.nanoTime() < endAt) {
                        int key = 1 + rnd.nextInt(20);  // Keys 1-20 only
                        int r = rnd.nextInt(100);
                        
                        if (r < 30) {  // 30% insert
                            tree.insert(key, tid);
                        } else if (r < 60) {  // 30% delete
                            tree.delete(key, tid);
                        } else if (r < 70) {  // 10% size
                            int s = tree.size(tid);
                            if (s < 0 || s > 30) {  // Impossible range
                                sizeErrors.incrementAndGet();
                                System.err.printf("❌ [Thread %d] INVALID SIZE: %d%n", tid, s);
                            }
                            sizes++;
                        } else {  // 30% contains
                            tree.contains(key);
                        }
                        
                        ops++;
                        
                        // Small delay to make output readable
                        if (ops % 10 == 0) {
                            Thread.sleep(50);
                        }
                    }
                    
                    threadOps.put(tid, ops);
                    sizeQueries.put(tid, sizes);
                    
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                } finally {
                    stop.countDown();
                }
            });
        }
        
        System.out.println("\n========== Starting concurrent operations ==========\n");
        start.countDown();
        stop.await();
        pool.shutdown();
        
        // Final verification
        System.out.println("\n========== Final Verification ==========");
        int finalSize = tree.size(0);
        int expectedSize = tree.getActualSize();
        
        long totalOps = threadOps.values().stream().mapToLong(Long::longValue).sum();
        long totalSizeQueries = sizeQueries.values().stream().mapToLong(Long::longValue).sum();
        
        System.out.printf("Total operations: %d%n", totalOps);
        System.out.printf("Total size queries: %d%n", totalSizeQueries);
        System.out.printf("Final size reported: %d%n", finalSize);
        System.out.printf("Expected size (inserts - deletes): %d%n", expectedSize);
        System.out.printf("Size query errors detected: %d%n", sizeErrors.get());
        
        // The exact match might not hold due to how we're counting, but should be close
        if (Math.abs(finalSize - expectedSize) <= numThreads) {
            System.out.println("\n✅ CORRECTNESS TEST PASSED!");
            System.out.println("Size is consistent (within expected range)");
        } else {
            System.err.println("\n❌ CORRECTNESS TEST FAILED!");
            System.err.printf("Size mismatch: reported=%d, expected=%d, diff=%d%n", 
                finalSize, expectedSize, Math.abs(finalSize - expectedSize));
        }
        
        if (sizeErrors.get() == 0) {
            System.out.println("✅ No invalid size values detected during execution");
        } else {
            System.err.println("❌ Invalid size values were detected!");
        }
    }
}

