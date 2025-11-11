package bst;

import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

import java.util.*;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Tests for rank and select queries using fastSize in nodes.
 */
public class RankSelectTest {

    @Test
    void testBasicRankQuery() {
        MyBST<Integer, String> bst = new MyBST<>();
        
        // Insert keys in random order
        int[] keys = {50, 25, 75, 10, 30, 60, 80, 5, 15, 27, 35};
        for (int key : keys) {
            bst.putIfAbsent(key, "value" + key);
        }
        
        // Sort to get expected ranks
        Arrays.sort(keys);
        
        // Test rank for each key
        for (int i = 0; i < keys.length; i++) {
            int expectedRank = i + 1; // 1-based
            int actualRank = bst.rank(keys[i]);
            assertEquals(expectedRank, actualRank, 
                "Rank of " + keys[i] + " should be " + expectedRank);
        }
        
        // Test rank for non-existent keys
        assertEquals(-1, bst.rank(999), "Rank of non-existent key should be -1");
        assertEquals(-1, bst.rank(1), "Rank of non-existent key should be -1");
        
        System.out.println("✓ Basic rank query test passed");
    }

    @Test
    void testBasicSelectQuery() {
        MyBST<Integer, String> bst = new MyBST<>();
        
        // Insert keys
        int[] keys = {50, 25, 75, 10, 30, 60, 80, 5, 15, 27, 35};
        for (int key : keys) {
            bst.putIfAbsent(key, "value" + key);
        }
        
        // Sort to get expected order
        Arrays.sort(keys);
        
        // Test select for each position
        for (int i = 0; i < keys.length; i++) {
            int k = i + 1; // 1-based
            Integer selected = bst.select(k);
            assertEquals(keys[i], selected, 
                "Select(" + k + ") should return " + keys[i]);
        }
        
        // Test select for out of range
        assertNull(bst.select(0), "Select(0) should return null");
        assertNull(bst.select(keys.length + 1), "Select(out of range) should return null");
        assertNull(bst.select(-1), "Select(negative) should return null");
        
        System.out.println("✓ Basic select query test passed");
    }

    @Test
    void testRankSelectConsistency() {
        MyBST<Integer, String> bst = new MyBST<>();
        
        // Insert keys
        for (int i = 0; i < 20; i++) {
            bst.putIfAbsent(i * 5, "value" + (i * 5));
        }
        
        // For each key, rank(select(k)) should equal k
        int size = bst.sizeSnapshot();
        for (int k = 1; k <= size; k++) {
            Integer key = bst.select(k);
            assertNotNull(key, "Select(" + k + ") should return a key");
            
            int rank = bst.rank(key);
            assertEquals(k, rank, 
                "rank(select(" + k + ")) should equal " + k + ", got rank=" + rank + " for key=" + key);
        }
        
        System.out.println("✓ Rank-select consistency test passed");
    }

    @Test
    void testRankWithSequentialKeys() {
        MyBST<Integer, String> bst = new MyBST<>();
        
        // Insert sequential keys
        for (int i = 1; i <= 10; i++) {
            bst.putIfAbsent(i, "value" + i);
        }
        
        // Test that ranks are correct
        for (int i = 1; i <= 10; i++) {
            assertEquals(i, bst.rank(i), "Rank of " + i + " should be " + i);
        }
        
        System.out.println("✓ Rank with sequential keys test passed");
    }

    @Test
    void testSelectWithSequentialKeys() {
        MyBST<Integer, String> bst = new MyBST<>();
        
        // Insert sequential keys
        for (int i = 1; i <= 10; i++) {
            bst.putIfAbsent(i, "value" + i);
        }
        
        // Test that selects are correct
        for (int i = 1; i <= 10; i++) {
            assertEquals(i, bst.select(i), "Select(" + i + ") should return " + i);
        }
        
        System.out.println("✓ Select with sequential keys test passed");
    }

    @Test
    void testRankSelectAfterDeletes() {
        MyBST<Integer, String> bst = new MyBST<>();
        
        // Insert keys
        List<Integer> keys = new ArrayList<>();
        for (int i = 0; i < 20; i++) {
            keys.add(i * 5);
            bst.putIfAbsent(i * 5, "value" + (i * 5));
        }
        
        // Delete some keys
        bst.remove(10);
        bst.remove(25);
        bst.remove(50);
        keys.remove(Integer.valueOf(10));
        keys.remove(Integer.valueOf(25));
        keys.remove(Integer.valueOf(50));
        
        Collections.sort(keys);
        
        // Test ranks after deletion
        for (int i = 0; i < keys.size(); i++) {
            int expectedRank = i + 1;
            int actualRank = bst.rank(keys.get(i));
            assertEquals(expectedRank, actualRank, 
                "After deletes, rank of " + keys.get(i) + " should be " + expectedRank);
        }
        
        // Test selects after deletion
        for (int i = 0; i < keys.size(); i++) {
            Integer selected = bst.select(i + 1);
            assertEquals(keys.get(i), selected, 
                "After deletes, select(" + (i + 1) + ") should return " + keys.get(i));
        }
        
        // Verify deleted keys return -1 for rank
        assertEquals(-1, bst.rank(10), "Deleted key should have rank -1");
        assertEquals(-1, bst.rank(25), "Deleted key should have rank -1");
        assertEquals(-1, bst.rank(50), "Deleted key should have rank -1");
        
        System.out.println("✓ Rank-select after deletes test passed");
    }

    @Test
    void testRankSelectWithConcurrentInserts() throws InterruptedException {
        MyBST<Integer, String> bst = new MyBST<>();
        int numThreads = 4;
        int insertsPerThread = 25;
        
        CountDownLatch startLatch = new CountDownLatch(1);
        CountDownLatch doneLatch = new CountDownLatch(numThreads);
        List<Integer> allKeys = Collections.synchronizedList(new ArrayList<>());
        
        for (int t = 0; t < numThreads; t++) {
            final int threadId = t;
            new Thread(() -> {
                try {
                    startLatch.await();
                    for (int i = 0; i < insertsPerThread; i++) {
                        int key = threadId * 1000 + i;
                        if (bst.putIfAbsent(key, "value" + key) == null) {
                            allKeys.add(key);
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
        
        Collections.sort(allKeys);
        
        // Test ranks
        for (int i = 0; i < allKeys.size(); i++) {
            int expectedRank = i + 1;
            int actualRank = bst.rank(allKeys.get(i));
            assertEquals(expectedRank, actualRank, 
                "After concurrent inserts, rank of " + allKeys.get(i) + " should be " + expectedRank);
        }
        
        // Test selects
        for (int i = 0; i < Math.min(10, allKeys.size()); i++) {
            Integer selected = bst.select(i + 1);
            assertEquals(allKeys.get(i), selected, 
                "After concurrent inserts, select(" + (i + 1) + ") should return " + allKeys.get(i));
        }
        
        System.out.println("✓ Rank-select with concurrent inserts test passed");
    }

    @Test
    void testRankSelectWithMixedConcurrentOps() throws InterruptedException {
        MyBST<Integer, String> bst = new MyBST<>();
        int numThreads = 4;
        int opsPerThread = 30;
        
        CountDownLatch startLatch = new CountDownLatch(1);
        CountDownLatch doneLatch = new CountDownLatch(numThreads);
        Set<Integer> insertedKeys = Collections.synchronizedSet(new HashSet<>());
        Set<Integer> deletedKeys = Collections.synchronizedSet(new HashSet<>());
        
        for (int t = 0; t < numThreads; t++) {
            final int threadId = t;
            new Thread(() -> {
                try {
                    startLatch.await();
                    for (int i = 0; i < opsPerThread; i++) {
                        int key = threadId * 1000 + i;
                        
                        // Insert
                        if (bst.putIfAbsent(key, "value" + key) == null) {
                            insertedKeys.add(key);
                        }
                        
                        // Delete some (every 3rd)
                        if (i % 3 == 0) {
                            if (bst.remove(key) != null) {
                                deletedKeys.add(key);
                                insertedKeys.remove(key);
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
        
        Thread.sleep(100);
        
        List<Integer> finalKeys = new ArrayList<>(insertedKeys);
        Collections.sort(finalKeys);
        
        int size = bst.sizeSnapshot();
        assertEquals(finalKeys.size(), size, "Size should match number of keys");
        
        // Test a sample of ranks
        for (int i = 0; i < Math.min(20, finalKeys.size()); i++) {
            int expectedRank = i + 1;
            int actualRank = bst.rank(finalKeys.get(i));
            assertEquals(expectedRank, actualRank, 
                "After mixed ops, rank of " + finalKeys.get(i) + " should be " + expectedRank);
        }
        
        // Test a sample of selects
        for (int i = 0; i < Math.min(20, finalKeys.size()); i++) {
            Integer selected = bst.select(i + 1);
            assertEquals(finalKeys.get(i), selected, 
                "After mixed ops, select(" + (i + 1) + ") should return " + finalKeys.get(i));
        }
        
        // Verify deleted keys return -1
        for (Integer deletedKey : deletedKeys) {
            assertEquals(-1, bst.rank(deletedKey), 
                "Deleted key " + deletedKey + " should have rank -1");
        }
        
        System.out.println("✓ Rank-select with mixed concurrent ops test passed");
        System.out.println("  Final keys: " + finalKeys.size() + ", Deleted: " + deletedKeys.size());
    }

    @Test
    void testRankSelectEdgeCases() {
        MyBST<Integer, String> bst = new MyBST<>();
        
        // Test with single element
        bst.putIfAbsent(42, "value42");
        assertEquals(1, bst.rank(42), "Rank of only element should be 1");
        assertEquals(42, bst.select(1), "Select(1) should return only element");
        assertNull(bst.select(2), "Select(2) should be null with only 1 element");
        
        // Test with two elements
        bst.putIfAbsent(10, "value10");
        assertEquals(1, bst.rank(10), "Rank of 10 should be 1");
        assertEquals(2, bst.rank(42), "Rank of 42 should be 2");
        assertEquals(10, bst.select(1), "Select(1) should return 10");
        assertEquals(42, bst.select(2), "Select(2) should return 42");
        
        System.out.println("✓ Rank-select edge cases test passed");
    }

    @Test
    void testSelectAllPositions() {
        MyBST<Integer, String> bst = new MyBST<>();
        
        // Insert keys
        int[] keys = {15, 8, 23, 4, 12, 19, 27, 2, 6, 10, 14, 17, 21, 25, 30};
        for (int key : keys) {
            bst.putIfAbsent(key, "value" + key);
        }
        
        Arrays.sort(keys);
        
        // Verify we can select all positions
        for (int pos = 1; pos <= keys.length; pos++) {
            Integer selected = bst.select(pos);
            assertNotNull(selected, "Select(" + pos + ") should not be null");
            assertEquals(keys[pos - 1], selected, 
                "Select(" + pos + ") should return " + keys[pos - 1]);
        }
        
        System.out.println("✓ Select all positions test passed");
    }

    @Test
    void testRankAllKeys() {
        MyBST<Integer, String> bst = new MyBST<>();
        
        // Insert keys
        int[] keys = {15, 8, 23, 4, 12, 19, 27, 2, 6, 10, 14, 17, 21, 25, 30};
        for (int key : keys) {
            bst.putIfAbsent(key, "value" + key);
        }
        
        Arrays.sort(keys);
        
        // Verify ranks for all keys
        for (int i = 0; i < keys.length; i++) {
            int rank = bst.rank(keys[i]);
            assertEquals(i + 1, rank, 
                "Rank of " + keys[i] + " should be " + (i + 1));
        }
        
        System.out.println("✓ Rank all keys test passed");
    }
}
