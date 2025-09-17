package bst;

import org.junit.jupiter.api.RepeatedTest;
import org.junit.jupiter.api.Test;

import java.util.*;
import static org.junit.jupiter.api.Assertions.*;

class BSTBasicTests {

    @Test
    void sanity_myBST_behavesLikeMap() {
        MyBST<Integer,Integer> map = new MyBST<>();

        // empty
        assertNull(map.get(5));

        // insert new (returns old value -> null)
        assertNull(map.put(5, 50));
        assertEquals(50, map.get(5));

        // update existing (returns old value)
        assertEquals(50, map.put(5, 55));
        assertEquals(55, map.get(5));

        // putIfAbsent should NOT overwrite and must return existing value
        assertEquals(55, map.putIfAbsent(5, 99));
        assertEquals(55, map.get(5));

        // remove returns the VALUE stored for the key
        assertEquals(55, map.remove(5));
        assertNull(map.get(5));
        assertNull(map.remove(5)); // removing again yields null
    }

    @Test
    void snapshot_size_and_select_are_consistent() {
        MyBST<Integer,Integer> t = new MyBST<>();

        // Insert a set of keys with value = k*10
        int[] keys = {5, 2, 8, 1, 3, 7, 9};
        for (int k : keys) t.putIfAbsent(k, k * 10);

        // Structural vs snapshot size
        assertEquals(keys.length, t.sizeStructural(), "structural size");
        assertEquals(keys.length, t.sizeSnapshot(),   "snapshot size");

        // containsKeySnapshot must agree with get/containsKey
        for (int k : keys) {
            assertTrue(t.containsKeySnapshot(k));
            assertEquals(k * 10, t.get(k));
        }
        assertFalse(t.containsKeySnapshot(42));
        assertNull(t.get(42));

        // selectKth must return sorted order (1-based)
        Arrays.sort(keys);
        for (int i = 0; i < keys.length; i++) {
            assertEquals(keys[i], t.selectKth(i + 1), "kth=" + (i + 1));
        }

        // After delete, sizes + snapshots update
        // IMPORTANT: remove returns the STORED VALUE (k*10), so removing 3 returns 30
        assertEquals(30, t.remove(3));
        assertFalse(t.containsKeySnapshot(3));
        assertNull(t.get(3));
        assertEquals(keys.length - 1, t.sizeSnapshot());
    }

    @Test
    void putIfAbsent_does_not_overwrite_and_propagates_snapshot() {
        MyBST<Integer,Integer> t = new MyBST<>();
        assertNull(t.putIfAbsent(10, 100));         // inserted
        assertEquals(100, t.putIfAbsent(10, 999));  // returns old, no overwrite
        assertEquals(100, t.get(10));               // unchanged
        assertTrue(t.containsKeySnapshot(10));      // snapshot reflects presence
        assertEquals(1, t.sizeSnapshot());          // snapshot size is correct
    }

    @Test
    void remove_updates_snapshot() {
        MyBST<Integer,Integer> t = new MyBST<>();
        for (int k : new int[]{4, 2, 6, 1, 3, 5, 7}) t.putIfAbsent(k, k); // value = k

        assertEquals(7, t.sizeSnapshot());
        assertTrue(t.containsKeySnapshot(6));

        // remove returns VALUE (here equals key)
        assertEquals(6, t.remove(6));
        assertFalse(t.containsKeySnapshot(6));
        assertEquals(6, t.sizeSnapshot());

        // Removing a non-present key should not throw and should keep snapshot consistent
        assertNull(t.remove(42));
        assertEquals(6, t.sizeSnapshot());
    }

    @RepeatedTest(3)
    void randomized_vs_TreeMap() {
        MyBST<Integer,Integer> t = new MyBST<>();
        TreeMap<Integer,Integer> ref = new TreeMap<>();
        Random rnd = new Random(12345);

        for (int i = 0; i < 2000; i++) {
            int k = rnd.nextInt(200);
            int op = rnd.nextInt(3);

            if (op == 0) { // upsert (put) with value = k*7
                Integer prev = ref.put(k, k*7);
                Integer got  = t.put(k, k*7);
                assertEquals(prev, got, "put returned old value");
            } else if (op == 1) { // putIfAbsent with value = k*9
                Integer prev = ref.get(k);
                if (prev == null) {
                    assertNull(t.putIfAbsent(k, k*9)); // inserted
                    ref.put(k, k*9);
                } else {
                    assertEquals(prev, t.putIfAbsent(k, 11111)); // returns existing
                }
            } else { // remove
                Integer prev = ref.remove(k);
                Integer r = t.remove(k);
                assertEquals(prev, r, "remove returned");
            }

            // Check snapshot size after each op
            assertEquals(ref.size(), t.sizeSnapshot(), "snapshot size");

            // Quick contains check for a couple of keys
            int a = rnd.nextInt(200), b = rnd.nextInt(200);
            assertEquals(ref.containsKey(a), t.containsKeySnapshot(a), "contains " + a);
            assertEquals(ref.containsKey(b), t.containsKeySnapshot(b), "contains " + b);

            // Cross-check selectKth against ref order
            if (!ref.isEmpty()) {
                int rank = 1 + rnd.nextInt(ref.size());
                Integer kth = nthKey(ref, rank);
                assertEquals(kth, t.selectKth(rank), "selectKth(" + rank + ")");
            } else {
                assertNull(t.selectKth(1));
            }
        }

        // Final full-order check via selectKth
        ArrayList<Integer> keys = new ArrayList<>(ref.keySet());
        for (int i = 0; i < keys.size(); i++) {
            assertEquals(keys.get(i), t.selectKth(i + 1));
        }
        assertNull(t.selectKth(keys.size() + 1));
    }

    /* ---- helpers ---- */

    private static <K extends Comparable<? super K>,V> K nthKey(TreeMap<K,V> tm, int n1based) {
        int i = 1;
        for (K k : tm.keySet()) {
            if (i++ == n1based) return k;
        }
        return null;
    }
}
