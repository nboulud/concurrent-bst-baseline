package bst;
import bst.MyBSTnext;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;

public class TestMyBSTnextConcurrent {
    
    public static void main(String[] args) throws Exception {
        System.out.println("=== Testing MyBSTnext Concurrent Operations ===\n");
        
        testConcurrentInserts();
        testConcurrentMixed();
        testConcurrentQueriesWithUpdates();
        testStressTest();
        
        System.out.println("\n=== ALL CONCURRENT TESTS PASSED ===");
    }
    
    static void testConcurrentInserts() throws Exception {
        System.out.println("Test 1: Concurrent inserts");
        MyBSTnext<Integer, String> bst = new MyBSTnext<>();
        int numThreads = 8;
        int insertsPerThread = 1000;
        
        ExecutorService executor = Executors.newFixedThreadPool(numThreads);
        CountDownLatch latch = new CountDownLatch(numThreads);
        
        for (int t = 0; t < numThreads; t++) {
            final int threadId = t;
            executor.submit(() -> {
                try {
                    for (int i = 0; i < insertsPerThread; i++) {
                        int key = threadId * insertsPerThread + i;
                        bst.putIfAbsent(key, "value" + key);
                    }
                } finally {
                    latch.countDown();
                }
            });
        }
        
        latch.await();
        executor.shutdown();
        
        int expectedSize = numThreads * insertsPerThread;
        int actualSize = bst.sizeSnapshot();
        assert actualSize == expectedSize : "Expected " + expectedSize + " but got " + actualSize;
        
        // Verify all keys exist
        for (int i = 0; i < expectedSize; i++) {
            assert bst.get(i) != null : "Key " + i + " should exist";
        }
        
        System.out.println("✓ Inserted " + expectedSize + " keys concurrently, size = " + actualSize);
    }
    
    static void testConcurrentMixed() throws Exception {
        System.out.println("\nTest 2: Concurrent mixed operations (insert/delete/get)");
        MyBSTnext<Integer, String> bst = new MyBSTnext<>();
        
        // Pre-fill with some data
        for (int i = 0; i < 10000; i++) {
            bst.putIfAbsent(i, "value" + i);
        }
        
        int numThreads = 8;
        int opsPerThread = 5000;
        ExecutorService executor = Executors.newFixedThreadPool(numThreads);
        CountDownLatch latch = new CountDownLatch(numThreads);
        AtomicInteger errors = new AtomicInteger(0);
        
        for (int t = 0; t < numThreads; t++) {
            executor.submit(() -> {
                try {
                    Random rand = new Random();
                    for (int i = 0; i < opsPerThread; i++) {
                        int op = rand.nextInt(100);
                        int key = rand.nextInt(20000);
                        
                        if (op < 30) {
                            // Insert
                            bst.putIfAbsent(key, "value" + key);
                        } else if (op < 50) {
                            // Delete
                            bst.remove(key);
                        } else {
                            // Get
                            bst.get(key);
                        }
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                    errors.incrementAndGet();
                } finally {
                    latch.countDown();
                }
            });
        }
        
        latch.await();
        executor.shutdown();
        
        assert errors.get() == 0 : "Encountered " + errors.get() + " errors";
        System.out.println("✓ Completed " + (numThreads * opsPerThread) + " mixed operations without errors");
    }
    
    static void testConcurrentQueriesWithUpdates() throws Exception {
        System.out.println("\nTest 3: Concurrent queries (rank/select/size) with updates");
        MyBSTnext<Integer, String> bst = new MyBSTnext<>();
        
        // Pre-fill
        for (int i = 0; i < 5000; i++) {
            bst.putIfAbsent(i * 2, "value" + (i * 2));
        }
        
        int numUpdaters = 4;
        int numQueriers = 4;
        int duration = 2; // seconds
        
        ExecutorService executor = Executors.newFixedThreadPool(numUpdaters + numQueriers);
        CountDownLatch startLatch = new CountDownLatch(1);
        CountDownLatch endLatch = new CountDownLatch(numUpdaters + numQueriers);
        AtomicBoolean stop = new AtomicBoolean(false);
        AtomicInteger errors = new AtomicInteger(0);
        AtomicInteger queryCount = new AtomicInteger(0);
        
        // Start updaters
        for (int t = 0; t < numUpdaters; t++) {
            executor.submit(() -> {
                try {
                    startLatch.await();
                    Random rand = new Random();
                    while (!stop.get()) {
                        int key = rand.nextInt(15000);
                        if (rand.nextBoolean()) {
                            bst.putIfAbsent(key, "value" + key);
                        } else {
                            bst.remove(key);
                        }
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                    errors.incrementAndGet();
                } finally {
                    endLatch.countDown();
                }
            });
        }
        
        // Start queriers
        for (int t = 0; t < numQueriers; t++) {
            executor.submit(() -> {
                try {
                    startLatch.await();
                    Random rand = new Random();
                    while (!stop.get()) {
                        int op = rand.nextInt(3);
                        if (op == 0) {
                            // size query
                            int size = bst.sizeSnapshot();
                            assert size >= 0 : "Size should be non-negative";
                        } else if (op == 1) {
                            // rank query
                            int key = rand.nextInt(15000);
                            int rank = bst.rank(key);
                            // rank can be -1 if key doesn't exist
                        } else {
                            // select query
                            int k = rand.nextInt(5000) + 1;
                            Integer key = bst.select(k);
                            // key can be null if k is out of range
                        }
                        queryCount.incrementAndGet();
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                    errors.incrementAndGet();
                } finally {
                    endLatch.countDown();
                }
            });
        }
        
        // Run test
        startLatch.countDown();
        Thread.sleep(duration * 1000);
        stop.set(true);
        endLatch.await();
        executor.shutdown();
        
        assert errors.get() == 0 : "Encountered " + errors.get() + " errors";
        System.out.println("✓ Completed " + queryCount.get() + " queries concurrently with updates");
    }
    
    static void testStressTest() throws Exception {
        System.out.println("\nTest 4: Stress test with heavy contention");
        MyBSTnext<Integer, String> bst = new MyBSTnext<>();
        
        int numThreads = 16;
        int duration = 3; // seconds
        
        ExecutorService executor = Executors.newFixedThreadPool(numThreads);
        CountDownLatch startLatch = new CountDownLatch(1);
        CountDownLatch endLatch = new CountDownLatch(numThreads);
        AtomicBoolean stop = new AtomicBoolean(false);
        AtomicInteger errors = new AtomicInteger(0);
        AtomicLong totalOps = new AtomicLong(0);
        
        for (int t = 0; t < numThreads; t++) {
            final int threadId = t;
            executor.submit(() -> {
                try {
                    startLatch.await();
                    Random rand = new Random();
                    long ops = 0;
                    while (!stop.get()) {
                        int op = rand.nextInt(100);
                        int key = rand.nextInt(1000); // High contention on small key range
                        
                        if (op < 25) {
                            bst.putIfAbsent(key, "value" + key);
                        } else if (op < 45) {
                            bst.remove(key);
                        } else if (op < 70) {
                            bst.get(key);
                        } else if (op < 85) {
                            bst.rank(key);
                        } else if (op < 95) {
                            bst.select(rand.nextInt(500) + 1);
                        } else {
                            bst.sizeSnapshot();
                        }
                        ops++;
                    }
                    totalOps.addAndGet(ops);
                } catch (Exception e) {
                    e.printStackTrace();
                    errors.incrementAndGet();
                } finally {
                    endLatch.countDown();
                }
            });
        }
        
        // Run test
        startLatch.countDown();
        Thread.sleep(duration * 1000);
        stop.set(true);
        endLatch.await();
        executor.shutdown();
        
        assert errors.get() == 0 : "Encountered " + errors.get() + " errors";
        long opsPerSecond = totalOps.get() / duration;
        System.out.println("✓ Stress test completed: " + totalOps.get() + " total ops (" + opsPerSecond + " ops/sec)");
        System.out.println("  Final size: " + bst.sizeSnapshot());
    }
}
