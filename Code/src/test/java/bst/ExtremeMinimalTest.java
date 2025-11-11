package bst;

import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

class ExtremeMinimalTest {
    @Test
    void justCreateObject() {
        System.out.println("=== Starting test ===");
        System.out.println("Creating MyBST...");
        MyBST<Integer, Integer> map = new MyBST<>();
        System.out.println("Object created successfully!");
        System.out.println("=== Test passed ===");
    }
}

