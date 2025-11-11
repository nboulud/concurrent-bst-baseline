package bst;

import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

/**
 * Minimal test - just basic put/get/remove without snapshot operations
 */
class MinimalTest {

    @Test
    void justPutAndGet() {
        System.out.println("Creating MyBST...");
        MyBST<Integer, Integer> map = new MyBST<>();
        System.out.println("MyBST created");
        
        System.out.println("Putting 5=50...");
        assertNull(map.putIfAbsent(5, 50));
        System.out.println("Put done");
        
        System.out.println("Getting 5...");
        Integer val = map.get(5);
        System.out.println("Get returned: " + val);
        assertEquals(50, val);
        
        System.out.println("Putting 10=100...");
        assertNull(map.putIfAbsent(10, 100));
        System.out.println("Put done");
        
        System.out.println("Getting 10...");
        val = map.get(10);
        System.out.println("Get returned: " + val);
        assertEquals(100, val);
        
        System.out.println("Removing 5...");
        Integer removed = map.remove(5);
        System.out.println("Remove returned: " + removed);
        assertEquals(50, removed);
        
        System.out.println("Getting 5 after remove...");
        val = map.get(5);
        System.out.println("Get returned: " + val);
        assertNull(val);
        
        System.out.println("=== Test completed successfully! ===");
    }
}

