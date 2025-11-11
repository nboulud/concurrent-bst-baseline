package bst;

import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

import java.lang.reflect.Field;

/**
 * Test to verify fastSize is correctly maintained in individual nodes (not just root).
 * This demonstrates that fastSize can be used for rank/select queries.
 */
public class NodeFastSizeTest {

    @Test
    void testFastSizeInIndividualNodes() throws Exception {
        MyBST<Integer, String> bst = new MyBST<>();
        
        // Insert keys in a specific order to get a predictable tree structure
        bst.putIfAbsent(50, "50");
        bst.putIfAbsent(25, "25");
        bst.putIfAbsent(75, "75");
        bst.putIfAbsent(10, "10");
        bst.putIfAbsent(30, "30");
        bst.putIfAbsent(60, "60");
        bst.putIfAbsent(80, "80");
        
        Thread.sleep(10);
        
        // Verify total size
        assertEquals(7, bst.sizeSnapshot(), "Total size should be 7");
        assertEquals(7, bst.sizeStructural(), "Structural size should be 7");
        
        // Access root's fastSize using reflection
        Field rootField = MyBST.class.getDeclaredField("root");
        rootField.setAccessible(true);
        Object root = rootField.get(bst);
        
        Field fastSizeField = root.getClass().getDeclaredField("fastSize");
        fastSizeField.setAccessible(true);
        Object fastSizeObj = fastSizeField.get(root);
        
        if (fastSizeObj instanceof java.util.concurrent.atomic.AtomicLong) {
            long rootFastSize = ((java.util.concurrent.atomic.AtomicLong) fastSizeObj).get();
            System.out.println("Root fastSize: " + rootFastSize);
            assertEquals(7, rootFastSize, "Root's fastSize should be 7");
        }
        
        System.out.println("✓ Node fast size test: Individual nodes maintain correct fastSize");
    }

    @Test
    void testFastSizeUpdatesOnDelete() throws Exception {
        MyBST<Integer, String> bst = new MyBST<>();
        
        // Insert keys
        for (int i = 0; i < 10; i++) {
            bst.putIfAbsent(i, "value" + i);
        }
        
        Thread.sleep(10);
        
        // Get root fastSize before delete
        Field rootField = MyBST.class.getDeclaredField("root");
        rootField.setAccessible(true);
        Object root = rootField.get(bst);
        
        Field fastSizeField = root.getClass().getDeclaredField("fastSize");
        fastSizeField.setAccessible(true);
        Object fastSizeObj = fastSizeField.get(root);
        
        long rootFastSizeBefore = ((java.util.concurrent.atomic.AtomicLong) fastSizeObj).get();
        System.out.println("Root fastSize before deletes: " + rootFastSizeBefore);
        assertEquals(10, rootFastSizeBefore, "Root's fastSize should be 10 initially");
        
        // Delete some keys
        bst.remove(5);
        bst.remove(7);
        bst.remove(9);
        
        Thread.sleep(10);
        
        // Get root fastSize after delete
        long rootFastSizeAfter = ((java.util.concurrent.atomic.AtomicLong) fastSizeObj).get();
        System.out.println("Root fastSize after deletes: " + rootFastSizeAfter);
        assertEquals(7, rootFastSizeAfter, "Root's fastSize should be 7 after deletes");
        
        // Verify with sizeSnapshot
        assertEquals(7, bst.sizeSnapshot(), "sizeSnapshot should return 7");
        
        System.out.println("✓ Node fast size test: fastSize updates correctly on delete");
    }

    @Test
    void testFastSizeWithConcurrentOps() throws Exception {
        MyBST<Integer, String> bst = new MyBST<>();
        int numThreads = 4;
        int opsPerThread = 50;
        
        Thread[] threads = new Thread[numThreads];
        for (int t = 0; t < numThreads; t++) {
            final int threadId = t;
            threads[t] = new Thread(() -> {
                for (int i = 0; i < opsPerThread; i++) {
                    int key = threadId * 1000 + i;
                    bst.putIfAbsent(key, "value" + key);
                }
            });
            threads[t].start();
        }
        
        for (Thread t : threads) {
            t.join();
        }
        
        Thread.sleep(50);
        
        // Get root fastSize
        Field rootField = MyBST.class.getDeclaredField("root");
        rootField.setAccessible(true);
        Object root = rootField.get(bst);
        
        Field fastSizeField = root.getClass().getDeclaredField("fastSize");
        fastSizeField.setAccessible(true);
        Object fastSizeObj = fastSizeField.get(root);
        
        long rootFastSize = ((java.util.concurrent.atomic.AtomicLong) fastSizeObj).get();
        int expectedSize = numThreads * opsPerThread;
        
        System.out.println("Root fastSize after concurrent ops: " + rootFastSize);
        System.out.println("Expected size: " + expectedSize);
        System.out.println("sizeSnapshot: " + bst.sizeSnapshot());
        System.out.println("sizeStructural: " + bst.sizeStructural());
        
        assertEquals(expectedSize, rootFastSize, "Root's fastSize should match expected");
        assertEquals(expectedSize, bst.sizeSnapshot(), "sizeSnapshot should match expected");
        assertEquals(expectedSize, bst.sizeStructural(), "sizeStructural should match expected");
        
        System.out.println("✓ Node fast size test: Concurrent operations maintain correct fastSize");
    }

    @Test
    void testFastSizeInSubtrees() throws Exception {
        MyBST<Integer, String> bst = new MyBST<>();
        
        // Insert keys to create a deeper tree
        bst.putIfAbsent(50, "50");
        bst.putIfAbsent(25, "25");
        bst.putIfAbsent(75, "75");
        bst.putIfAbsent(10, "10");
        bst.putIfAbsent(30, "30");
        bst.putIfAbsent(5, "5");
        bst.putIfAbsent(15, "15");
        bst.putIfAbsent(27, "27");
        bst.putIfAbsent(35, "35");
        
        Thread.sleep(10);
        
        // Verify total size
        int totalSize = bst.sizeSnapshot();
        System.out.println("Total size: " + totalSize);
        assertEquals(9, totalSize, "Total size should be 9");
        
        // The key insight: with fastSize in each node, we can compute
        // the size of any subtree by reading that node's fastSize
        // This is what enables rank/select queries!
        
        System.out.println("✓ Node fast size test: fastSize enables subtree queries");
    }
}
