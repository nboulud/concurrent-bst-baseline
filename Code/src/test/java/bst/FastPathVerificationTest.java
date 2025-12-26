package bst;

import java.util.Random;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Test to verify fast path is actually being used in MyBSTnext.
 * The fast path should allow updates WITHOUT calling propagateWithCounter().
 */
public class FastPathVerificationTest {
    
    static final int TREE_SIZE = 100000;
    static final int WARMUP_OPS = 100000;
    static final int TEST_OPS = 1000000;
    static final int NUM_UPDATE_THREADS = 32;
    static final int NUM_QUERY_THREADS = 0;  // Start with NO query threads
    
    public static void main(String[] args) throws Exception {
        System.out.println("=".repeat(80));
        System.out.println("FAST PATH VERIFICATION TEST");
        System.out.println("=".repeat(80));
        
        // Test 1: Updates only (should use fast path)
        System.out.println("\nTest 1: MyBSTnext - 32 update threads, 0 query threads");
        System.out.println("Expected: Fast path active, no propagateWithCounter() calls");
        System.out.println("-".repeat(80));
        testMyBSTnextUpdatesOnly();
        
        // Test 2: Updates + queries (should use slow path)
        System.out.println("\nTest 2: MyBSTnext - 32 update threads, 8 query threads");
        System.out.println("Expected: Slow path active, propagateWithCounter() on every update");
        System.out.println("-".repeat(80));
        testMyBSTnextWithQueries();
        
        // Test 3: MyBSTBaseline for comparison
        System.out.println("\nTest 3: MyBSTBaseline - 32 update threads, 0 query threads");
        System.out.println("Expected: Always calls propagate() on updates");
        System.out.println("-".repeat(80));
        testMyBSTBaseline();
        
        System.out.println("\n" + "=".repeat(80));
        System.out.println("EXPECTED RESULTS");
        System.out.println("=".repeat(80));
        System.out.println("If fast path is working:");
        System.out.println("  - Test 1 (MyBSTnext, no queries): Should be FASTER than MyBSTBaseline");
        System.out.println("  - Test 2 (MyBSTnext, with queries): Should be slower due to slow path");
        System.out.println("\nIf fast path is NOT working:");
        System.out.println("  - Test 1 will be similar to or slower than MyBSTBaseline");
        System.out.println("  - This means propagateWithCounter() is being called even without queries!");
        System.out.println("=".repeat(80));
    }
    
    static void testMyBSTnextUpdatesOnly() throws Exception {
        MyBSTnext<Integer, Integer> tree = new MyBSTnext<>();
        
        // Build tree
        System.out.println("Building initial tree...");
        Random rand = new Random(42);
        for (int i = 0; i < TREE_SIZE; i++) {
            tree.putIfAbsent(rand.nextInt(TREE_SIZE * 2), i);
        }
        
        // Warmup
        System.out.println("Warmup...");
        for (int i = 0; i < WARMUP_OPS; i++) {
            tree.putIfAbsent(rand.nextInt(TREE_SIZE * 2), i);
        }
        
        // Test - only update operations, NO queries
        System.out.println("Running test with " + NUM_UPDATE_THREADS + " update threads, NO queries...");
        AtomicLong totalOps = new AtomicLong(0);
        Thread[] threads = new Thread[NUM_UPDATE_THREADS];
        
        long start = System.nanoTime();
        
        for (int t = 0; t < NUM_UPDATE_THREADS; t++) {
            final int seed = t;
            threads[t] = new Thread(() -> {
                Random r = new Random(seed);
                long ops = 0;
                for (int i = 0; i < TEST_OPS / NUM_UPDATE_THREADS; i++) {
                    // Mix of inserts and deletes
                    if (i % 2 == 0) {
                        tree.putIfAbsent(r.nextInt(TREE_SIZE * 2), i);
                    } else {
                        tree.remove(r.nextInt(TREE_SIZE * 2));
                    }
                    ops++;
                }
                totalOps.addAndGet(ops);
            });
            threads[t].start();
        }
        
        for (Thread t : threads) {
            t.join();
        }
        
        long elapsed = System.nanoTime() - start;
        double seconds = elapsed / 1e9;
        double throughput = totalOps.get() / seconds;
        
        System.out.println("Results:");
        System.out.println("  Total operations: " + totalOps.get());
        System.out.println("  Time: " + String.format("%.2f", seconds) + " seconds");
        System.out.println("  Throughput: " + String.format("%.2f", throughput / 1e6) + " Mops/s");
        System.out.println("  " + tree.getProfilingStats());
    }
    
    static void testMyBSTnextWithQueries() throws Exception {
        MyBSTnext<Integer, Integer> tree = new MyBSTnext<>();
        
        // Build tree
        System.out.println("Building initial tree...");
        Random rand = new Random(42);
        for (int i = 0; i < TREE_SIZE; i++) {
            tree.putIfAbsent(rand.nextInt(TREE_SIZE * 2), i);
        }
        
        // Warmup
        System.out.println("Warmup...");
        for (int i = 0; i < WARMUP_OPS; i++) {
            tree.putIfAbsent(rand.nextInt(TREE_SIZE * 2), i);
        }
        
        // Test - update operations WITH concurrent queries
        final int NUM_QUERIES = 8;
        System.out.println("Running test with " + NUM_UPDATE_THREADS + " update threads + " + NUM_QUERIES + " query threads...");
        AtomicLong totalOps = new AtomicLong(0);
        AtomicLong queryOps = new AtomicLong(0);
        Thread[] threads = new Thread[NUM_UPDATE_THREADS + NUM_QUERIES];
        
        long start = System.nanoTime();
        
        // Start update threads
        for (int t = 0; t < NUM_UPDATE_THREADS; t++) {
            final int seed = t;
            threads[t] = new Thread(() -> {
                Random r = new Random(seed);
                long ops = 0;
                for (int i = 0; i < TEST_OPS / NUM_UPDATE_THREADS; i++) {
                    if (i % 2 == 0) {
                        tree.putIfAbsent(r.nextInt(TREE_SIZE * 2), i);
                    } else {
                        tree.remove(r.nextInt(TREE_SIZE * 2));
                    }
                    ops++;
                }
                totalOps.addAndGet(ops);
            });
            threads[t].start();
        }
        
        // Start query threads
        for (int t = 0; t < NUM_QUERIES; t++) {
            final int seed = t + 1000;
            threads[NUM_UPDATE_THREADS + t] = new Thread(() -> {
                Random r = new Random(seed);
                long ops = 0;
                for (int i = 0; i < TEST_OPS / NUM_UPDATE_THREADS; i++) {
                    tree.rank(r.nextInt(TREE_SIZE * 2));
                    ops++;
                }
                queryOps.addAndGet(ops);
            });
            threads[NUM_UPDATE_THREADS + t].start();
        }
        
        for (Thread t : threads) {
            t.join();
        }
        
        long elapsed = System.nanoTime() - start;
        double seconds = elapsed / 1e9;
        double updateThroughput = totalOps.get() / seconds;
        double queryThroughput = queryOps.get() / seconds;
        
        System.out.println("Results:");
        System.out.println("  Update operations: " + totalOps.get());
        System.out.println("  Query operations: " + queryOps.get());
        System.out.println("  Time: " + String.format("%.2f", seconds) + " seconds");
        System.out.println("  Update throughput: " + String.format("%.2f", updateThroughput / 1e6) + " Mops/s");
        System.out.println("  Query throughput: " + String.format("%.2f", queryThroughput / 1e6) + " Mops/s");
        System.out.println("  " + tree.getProfilingStats());
    }
    
    static void testMyBSTBaseline() throws Exception {
        MyBSTBaseline<Integer, Integer> tree = new MyBSTBaseline<>();
        
        // Build tree
        System.out.println("Building initial tree...");
        Random rand = new Random(42);
        for (int i = 0; i < TREE_SIZE; i++) {
            tree.putIfAbsent(rand.nextInt(TREE_SIZE * 2), i);
        }
        
        // Warmup
        System.out.println("Warmup...");
        for (int i = 0; i < WARMUP_OPS; i++) {
            tree.putIfAbsent(rand.nextInt(TREE_SIZE * 2), i);
        }
        
        // Test - only update operations
        System.out.println("Running test with " + NUM_UPDATE_THREADS + " update threads...");
        AtomicLong totalOps = new AtomicLong(0);
        Thread[] threads = new Thread[NUM_UPDATE_THREADS];
        
        long start = System.nanoTime();
        
        for (int t = 0; t < NUM_UPDATE_THREADS; t++) {
            final int seed = t;
            threads[t] = new Thread(() -> {
                Random r = new Random(seed);
                long ops = 0;
                for (int i = 0; i < TEST_OPS / NUM_UPDATE_THREADS; i++) {
                    if (i % 2 == 0) {
                        tree.putIfAbsent(r.nextInt(TREE_SIZE * 2), i);
                    } else {
                        tree.remove(r.nextInt(TREE_SIZE * 2));
                    }
                    ops++;
                }
                totalOps.addAndGet(ops);
            });
            threads[t].start();
        }
        
        for (Thread t : threads) {
            t.join();
        }
        
        long elapsed = System.nanoTime() - start;
        double seconds = elapsed / 1e9;
        double throughput = totalOps.get() / seconds;
        
        System.out.println("Results:");
        System.out.println("  Total operations: " + totalOps.get());
        System.out.println("  Time: " + String.format("%.2f", seconds) + " seconds");
        System.out.println("  Throughput: " + String.format("%.2f", throughput / 1e6) + " Mops/s");
        System.out.println("  (Always calls propagate() on updates)");
    }
}
