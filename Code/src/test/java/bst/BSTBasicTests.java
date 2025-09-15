package bst;

import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

class BSTBasicTests {

    @Test
    void sanity_lockFreeBSTMap_behavesLikeMap() {
        LockFreeBSTMap<Integer,Integer> map = new LockFreeBSTMap<>();

        assertNull(map.get(5));            // empty
        assertNull(map.put(5, 50));        // insert new (returns old value -> null)
        assertEquals(50, map.get(5));      // read back
        assertEquals(50, map.put(5, 55));  // update existing (returns old value)
        assertEquals(55, map.get(5));
        assertEquals(55, map.remove(5));   // remove existing (returns removed value)
        assertNull(map.get(5));            // gone
    }

    @Test
    void sanity_baselineBST_behavesLikeMap() {
        // Your baseline BST is also a Map<K,V> (not a Set)
        BST<Integer,Integer> map = new BST<>();

        assertNull(map.get(7));
        assertNull(map.put(7, 70));        // insert
        assertEquals(70, map.get(7));
        assertEquals(70, map.put(7, 71));  // update returns old
        assertEquals(71, map.get(7));
        assertEquals(71, map.remove(7));   // remove returns value
        assertNull(map.get(7));
    }
}
