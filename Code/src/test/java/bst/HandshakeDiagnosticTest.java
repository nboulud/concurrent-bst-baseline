package bst;

import java.util.Random;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Simple diagnostic test to measure rank/select overhead.
 * Tests the impact of enterSlowPath()/exitSlowPath() synchronization.
 */
public class HandshakeDiagnosticTest {
    
    static final int TREE_SIZE = 100000;
    static final int WARMUP_OPS = 100000;
    static final int TEST_OPS = 1000000;
    static final int NUM_THREADS = 8;
    
    public static void main(String[] args) throws Exception {
        System.out.println("=".repeat(80));
        System.out.println("HANDSHAKE OVERHEAD DIAGNOSTIC TEST");
        System.out.println("=".repeat(80));
        
        // Test 1: MyBSTBaseline with concurrent rank operations
        System.out.println("\nTest 1: MyBSTBaseline - rank/select with NO handshakes");
        System.out.println("-".repeat(80));
        testMyBSTBaseline();
        
        // Test 2: MyBSTnext with concurrent rank operations  
        System.out.println("\nTest 2: MyBSTnext - rank/select WITH handshake protocol");
        System.out.println("-".repeat(80));
        testMyBSTnext();
        
        System.out.println("\n" + "=".repeat(80));
        System.out.println("ANALYSIS");
        System.out.println("=".repeat(80));
        System.out.println("If MyBSTnext is significantly slower, the enterSlowPath()/exitSlowPath()");
        System.out.println("handshake protocol is the bottleneck.");
        System.out.println("\nExpected: MyBSTnext will be 3-5x slower due to handshake overhead");
        System.out.println("=".repeat(80));
    }
    
    static void testMyBSTBaseline() throws Exception {
        MyBSTBaseline<Integer, Integer> tree = new MyBSTBaseline<>();
        
        // Build tree
        System.out.println("Building tree with " + TREE_SIZE + " keys...");
        Random rand = new Random(42);
        for (int i = 0; i < TREE_SIZE; i++) {
            tree.putIfAbsent(rand.nextInt(TREE_SIZE * 2), i);
        }
        
        // Warmup
        System.out.println("Warmup...");
        for (int i = 0; i < WARMUP_OPS; i++) {
            tree.rank(rand.nextInt(TREE_SIZE * 2));
        }
        
        // Test with concurrent rank operations
        System.out.println("Running test with " + NUM_THREADS + " threads...");
        AtomicLong totalOps = new AtomicLong(0);
        Thread[] threads = new Thread[NUM_THREADS];
        
        long start = System.nanoTime();
        
        for (int t = 0; t < NUM_THREADS; t++) {
            final int seed = t;
            threads[t] = new Thread(() -> {
                Random r = new Random(seed);
                long ops = 0;
                for (int i = 0; i < TEST_OPS / NUM_THREADS; i++) {
                    tree.rank(r.nextInt(TREE_SIZE * 2));
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
    }
    
    static void testMyBSTnext() throws Exception {
        MyBSTnext<Integer, Integer> tree = new MyBSTnext<>();
        
        // Build tree
        System.out.println("Building tree with " + TREE_SIZE + " keys...");
        Random rand = new Random(42);
        for (int i = 0; i < TREE_SIZE; i++) {
            tree.putIfAbsent(rand.nextInt(TREE_SIZE * 2), i);
        }
        
        // Warmup
        System.out.println("Warmup...");
        for (int i = 0; i < WARMUP_OPS; i++) {
            tree.rank(rand.nextInt(TREE_SIZE * 2));
        }
        
        // Test with concurrent rank operations
        System.out.println("Running test with " + NUM_THREADS + " threads...");
        AtomicLong totalOps = new AtomicLong(0);
        Thread[] threads = new Thread[NUM_THREADS];
        
        long start = System.nanoTime();
        
        for (int t = 0; t < NUM_THREADS; t++) {
            final int seed = t;
            threads[t] = new Thread(() -> {
                Random r = new Random(seed);
                long ops = 0;
                for (int i = 0; i < TEST_OPS / NUM_THREADS; i++) {
                    tree.rank(r.nextInt(TREE_SIZE * 2));
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
        
        // Print handshake statistics
        String stats = tree.getProfilingStats();
        System.out.println("  " + stats);
    }
}
