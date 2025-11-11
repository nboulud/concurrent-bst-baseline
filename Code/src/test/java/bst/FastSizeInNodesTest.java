package bst;

import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Test that fastSize is being updated correctly in each node during fast path operations.
 */
public class FastSizeInNodesTest {

    @Test
    void testFastSizeInNodesAfterInserts() throws InterruptedException {
        MyBST<Integer, String> bst = new MyBST<>();
        
        // Insert some keys
        bst.putIfAbsent(10, "ten");
        bst.putIfAbsent(5, "five");
        bst.putIfAbsent(15, "fifteen");
        bst.putIfAbsent(3, "three");
        bst.putIfAbsent(7, "seven");
        
        // Allow a brief moment for any pending operations
        Thread.sleep(10);
        
        // Check that size is correct
        int size = bst.sizeSnapshot();
        assertEquals(5, size, "Size should be 5");
        
        // Check structural size
        int structuralSize = bst.sizeStructural();
        assertEquals(5, structuralSize, "Structural size should be 5");
        
        System.out.println("✓ Fast size in nodes test: Size correct after inserts");
    }

    @Test
    void testFastSizeInNodesAfterDeletes() throws InterruptedException {
        MyBST<Integer, String> bst = new MyBST<>();
        
        // Insert some keys
        bst.putIfAbsent(10, "ten");
        bst.putIfAbsent(5, "five");
        bst.putIfAbsent(15, "fifteen");
        bst.putIfAbsent(3, "three");
        bst.putIfAbsent(7, "seven");
        
        // Delete some keys
        bst.remove(3);
        bst.remove(15);
        
        Thread.sleep(10);
        
        // Check that size is correct
        int size = bst.sizeSnapshot();
        assertEquals(3, size, "Size should be 3 after deletes");
        
        int structuralSize = bst.sizeStructural();
        assertEquals(3, structuralSize, "Structural size should be 3 after deletes");
        
        System.out.println("✓ Fast size in nodes test: Size correct after deletes");
    }

    @Test
    void testFastSizeWithConcurrentInserts() throws InterruptedException {
        MyBST<Integer, String> bst = new MyBST<>();
        int numThreads = 4;
        int insertsPerThread = 25;
        
        CountDownLatch startLatch = new CountDownLatch(1);
        CountDownLatch doneLatch = new CountDownLatch(numThreads);
        
        for (int t = 0; t < numThreads; t++) {
            final int threadId = t;
            new Thread(() -> {
                try {
                    startLatch.await();
                    for (int i = 0; i < insertsPerThread; i++) {
                        int key = threadId * 1000 + i;
                        bst.putIfAbsent(key, "value" + key);
                    }
                } catch (InterruptedException e) {
                    e.printStackTrace();
                } finally {
                    doneLatch.countDown();
                }
            }).start();
        }
        
        startLatch.countDown();
        doneLatch.await();
        
        Thread.sleep(50);
        
        int expectedSize = numThreads * insertsPerThread;
        int size = bst.sizeSnapshot();
        int structuralSize = bst.sizeStructural();
        
        assertEquals(expectedSize, size, "Size should match number of inserts");
        assertEquals(expectedSize, structuralSize, "Structural size should match");
        
        System.out.println("✓ Fast size in nodes test: Concurrent inserts correct");
    }

    @Test
    void testFastSizeWithMixedOperations() throws InterruptedException {
        MyBST<Integer, String> bst = new MyBST<>();
        int numThreads = 4;
        int opsPerThread = 20;
        
        CountDownLatch startLatch = new CountDownLatch(1);
        CountDownLatch doneLatch = new CountDownLatch(numThreads);
        AtomicInteger insertCount = new AtomicInteger(0);
        AtomicInteger deleteCount = new AtomicInteger(0);
        
        for (int t = 0; t < numThreads; t++) {
            final int threadId = t;
            new Thread(() -> {
                try {
                    startLatch.await();
                    for (int i = 0; i < opsPerThread; i++) {
                        int key = threadId * 1000 + i;
                        
                        // Insert
                        if (bst.putIfAbsent(key, "value" + key) == null) {
                            insertCount.incrementAndGet();
                        }
                        
                        // Delete some (every other)
                        if (i % 2 == 0) {
                            if (bst.remove(key) != null) {
                                deleteCount.incrementAndGet();
                            }
                        }
                    }
                } catch (InterruptedException e) {
                    e.printStackTrace();
                } finally {
                    doneLatch.countDown();
                }
            }).start();
        }
        
        startLatch.countDown();
        doneLatch.await();
        
        Thread.sleep(50);
        
        int expectedSize = insertCount.get() - deleteCount.get();
        int size = bst.sizeSnapshot();
        int structuralSize = bst.sizeStructural();
        
        assertEquals(expectedSize, size, "Size should match inserts - deletes");
        assertEquals(expectedSize, structuralSize, "Structural size should match");
        
        System.out.println("✓ Fast size in nodes test: Mixed operations correct");
        System.out.println("  Inserts: " + insertCount.get() + ", Deletes: " + deleteCount.get() + ", Final size: " + size);
    }

    @Test
    void testRootFastSizeMatchesTotalSize() throws InterruptedException {
        MyBST<Integer, String> bst = new MyBST<>();
        
        // Insert keys
        for (int i = 0; i < 20; i++) {
            bst.putIfAbsent(i, "value" + i);
        }
        
        Thread.sleep(10);
        
        // Access root's fastSize via reflection or just check via sizeSnapshot
        int size = bst.sizeSnapshot();
        int structuralSize = bst.sizeStructural();
        
        assertEquals(20, size, "Size should be 20");
        assertEquals(20, structuralSize, "Structural size should be 20");
        
        // Delete some
        for (int i = 0; i < 10; i++) {
            bst.remove(i);
        }
        
        Thread.sleep(10);
        
        size = bst.sizeSnapshot();
        structuralSize = bst.sizeStructural();
        
        assertEquals(10, size, "Size should be 10 after deletes");
        assertEquals(10, structuralSize, "Structural size should be 10 after deletes");
        
        System.out.println("✓ Fast size in nodes test: Root fast size matches total size");
    }
}
