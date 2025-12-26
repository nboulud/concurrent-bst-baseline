package bst;

import java.util.Random;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Test to investigate contains() performance degradation.
 * Measures impact of using Version tree vs direct BST navigation for contains.
 */
public class ContainsDegradationTest {
    
    static final int TREE_SIZE = 100000;
    static final int WARMUP_OPS = 100000;
    static final int TEST_OPS = 10000000;  // 10M for contains-heavy workload
    static final int NUM_THREADS = 32;
    
    public static void main(String[] args) throws Exception {
        System.out.println("=".repeat(80));
        System.out.println("CONTAINS DEGRADATION TEST");
        System.out.println("Testing the 3ins-2rem-95contains workload");
        System.out.println("=".repeat(80));
        
        // Test 1: MyBSTBaseline (always BST navigation)
        System.out.println("\nTest 1: MyBSTBaseline - always BST navigation for contains");
        System.out.println("-".repeat(80));
        testMyBSTBaseline();
        
        // Test 2: MyBSTnext without queries (BST navigation)
        System.out.println("\nTest 2: MyBSTnext - NO queries (phase 0, BST navigation)");
        System.out.println("-".repeat(80));
        testMyBSTnextNoQueries();
        
        // Test 3: MyBSTnext WITH queries (Version tree navigation for contains!)
        System.out.println("\nTest 3: MyBSTnext - WITH queries (phase 2, Version tree navigation)");
        System.out.println("-".repeat(80));
        testMyBSTnextWithQueries();
        
        System.out.println("\n" + "=".repeat(80));
        System.out.println("EXPECTED FINDINGS");
        System.out.println("=".repeat(80));
        System.out.println("If Version tree navigation is slower:");
        System.out.println("  - Test 3 should be MUCH slower than Tests 1 & 2");
        System.out.println("  - The 95% contains operations are using Version tree instead of BST!");
        System.out.println("  - This explains the massive performance drop");
        System.out.println("=".repeat(80));
    }
    
    static void testMyBSTBaseline() throws Exception {
        MyBSTBaseline<Integer, Integer> tree = new MyBSTBaseline<>();
        runContainsWorkload(tree, "MyBSTBaseline");
    }
    
    static void testMyBSTnextNoQueries() throws Exception {
        MyBSTnext<Integer, Integer> tree = new MyBSTnext<>();
        runContainsWorkload(tree, "MyBSTnext (no queries)");
    }
    
    static void testMyBSTnextWithQueries() throws Exception {
        final MyBSTnext<Integer, Integer> tree = new MyBSTnext<>();
        
        // Build tree
        System.out.println("Building tree...");
        Random rand = new Random(42);
        for (int i = 0; i < TREE_SIZE; i++) {
            tree.putIfAbsent(rand.nextInt(TREE_SIZE * 2), i);
        }
        
        // Warmup
        System.out.println("Warmup...");
        for (int i = 0; i < WARMUP_OPS; i++) {
            if (i % 100 < 3) tree.putIfAbsent(rand.nextInt(TREE_SIZE * 2), i);
            else if (i % 100 < 5) tree.remove(rand.nextInt(TREE_SIZE * 2));
            else tree.containsKey(rand.nextInt(TREE_SIZE * 2));
        }
        
        // Start query thread to keep system in slow path
        AtomicLong queryOps = new AtomicLong(0);
        final boolean[] running = {true};
        Thread queryThread = new Thread(() -> {
            Random r = new Random(999);
            while (running[0]) {
                tree.rank(r.nextInt(TREE_SIZE * 2));
                queryOps.incrementAndGet();
            }
        });
        queryThread.start();
        
        // Wait a bit to ensure we're in slow path
        Thread.sleep(100);
        
        System.out.println("Running workload (3% insert, 2% delete, 95% contains)...");
        AtomicLong totalOps = new AtomicLong(0);
        Thread[] threads = new Thread[NUM_THREADS];
        
        long start = System.nanoTime();
        
        for (int t = 0; t < NUM_THREADS; t++) {
            final int seed = t;
            threads[t] = new Thread(() -> {
                Random r = new Random(seed);
                long ops = 0;
                for (int i = 0; i < TEST_OPS / NUM_THREADS; i++) {
                    int op = i % 100;
                    if (op < 3) {
                        tree.putIfAbsent(r.nextInt(TREE_SIZE * 2), i);
                    } else if (op < 5) {
                        tree.remove(r.nextInt(TREE_SIZE * 2));
                    } else {
                        tree.containsKey(r.nextInt(TREE_SIZE * 2));
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
        
        running[0] = false;
        queryThread.join();
        
        long elapsed = System.nanoTime() - start;
        double seconds = elapsed / 1e9;
        double throughput = totalOps.get() / seconds;
        
        System.out.println("Results:");
        System.out.println("  Total operations: " + totalOps.get());
        System.out.println("  Query operations: " + queryOps.get());
        System.out.println("  Time: " + String.format("%.2f", seconds) + " seconds");
        System.out.println("  Throughput: " + String.format("%.2f", throughput / 1e6) + " Mops/s");
        System.out.println("  " + tree.getProfilingStats());
    }
    
    static void runContainsWorkload(Object tree, String name) throws Exception {
        // Build tree
        System.out.println("Building tree...");
        Random rand = new Random(42);
        for (int i = 0; i < TREE_SIZE; i++) {
            if (tree instanceof MyBSTBaseline) {
                ((MyBSTBaseline<Integer, Integer>) tree).putIfAbsent(rand.nextInt(TREE_SIZE * 2), i);
            } else {
                ((MyBSTnext<Integer, Integer>) tree).putIfAbsent(rand.nextInt(TREE_SIZE * 2), i);
            }
        }
        
        // Warmup
        System.out.println("Warmup...");
        for (int i = 0; i < WARMUP_OPS; i++) {
            if (i % 100 < 3) {
                if (tree instanceof MyBSTBaseline) {
                    ((MyBSTBaseline<Integer, Integer>) tree).putIfAbsent(rand.nextInt(TREE_SIZE * 2), i);
                } else {
                    ((MyBSTnext<Integer, Integer>) tree).putIfAbsent(rand.nextInt(TREE_SIZE * 2), i);
                }
            } else if (i % 100 < 5) {
                if (tree instanceof MyBSTBaseline) {
                    ((MyBSTBaseline<Integer, Integer>) tree).remove(rand.nextInt(TREE_SIZE * 2));
                } else {
                    ((MyBSTnext<Integer, Integer>) tree).remove(rand.nextInt(TREE_SIZE * 2));
                }
            } else {
                if (tree instanceof MyBSTBaseline) {
                    ((MyBSTBaseline<Integer, Integer>) tree).containsKey(rand.nextInt(TREE_SIZE * 2));
                } else {
                    ((MyBSTnext<Integer, Integer>) tree).containsKey(rand.nextInt(TREE_SIZE * 2));
                }
            }
        }
        
        System.out.println("Running workload (3% insert, 2% delete, 95% contains)...");
        AtomicLong totalOps = new AtomicLong(0);
        Thread[] threads = new Thread[NUM_THREADS];
        
        long start = System.nanoTime();
        
        for (int t = 0; t < NUM_THREADS; t++) {
            final int seed = t;
            final Object finalTree = tree;
            threads[t] = new Thread(() -> {
                Random r = new Random(seed);
                long ops = 0;
                for (int i = 0; i < TEST_OPS / NUM_THREADS; i++) {
                    int op = i % 100;
                    if (op < 3) {
                        if (finalTree instanceof MyBSTBaseline) {
                            ((MyBSTBaseline<Integer, Integer>) finalTree).putIfAbsent(r.nextInt(TREE_SIZE * 2), i);
                        } else {
                            ((MyBSTnext<Integer, Integer>) finalTree).putIfAbsent(r.nextInt(TREE_SIZE * 2), i);
                        }
                    } else if (op < 5) {
                        if (finalTree instanceof MyBSTBaseline) {
                            ((MyBSTBaseline<Integer, Integer>) finalTree).remove(r.nextInt(TREE_SIZE * 2));
                        } else {
                            ((MyBSTnext<Integer, Integer>) finalTree).remove(r.nextInt(TREE_SIZE * 2));
                        }
                    } else {
                        if (finalTree instanceof MyBSTBaseline) {
                            ((MyBSTBaseline<Integer, Integer>) finalTree).containsKey(r.nextInt(TREE_SIZE * 2));
                        } else {
                            ((MyBSTnext<Integer, Integer>) finalTree).containsKey(r.nextInt(TREE_SIZE * 2));
                        }
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
    }
}
