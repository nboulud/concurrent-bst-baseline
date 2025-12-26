import bst.MyBSTnext;

public class TestMyBSTnext {
    
    public static void main(String[] args) {
        System.out.println("=== Testing MyBSTnext ===");
        
        MyBSTnext<Integer, String> bst = new MyBSTnext<>();
        
        // Test 1: Basic insert and get
        System.out.println("\nTest 1: Basic insert and get");
        bst.putIfAbsent(5, "five");
        bst.putIfAbsent(3, "three");
        bst.putIfAbsent(7, "seven");
        bst.putIfAbsent(1, "one");
        bst.putIfAbsent(9, "nine");
        
        assert "five".equals(bst.get(5)) : "Failed to get 5";
        assert "three".equals(bst.get(3)) : "Failed to get 3";
        assert "seven".equals(bst.get(7)) : "Failed to get 7";
        System.out.println("✓ Insert and get working");
        
        // Test 2: Size query
        System.out.println("\nTest 2: Size query");
        int size = bst.sizeSnapshot();
        System.out.println("Size: " + size);
        assert size == 5 : "Expected size 5, got " + size;
        System.out.println("✓ Size query working");
        
        // Test 3: Rank query
        System.out.println("\nTest 3: Rank query");
        int rank3 = bst.rank(3);
        int rank5 = bst.rank(5);
        int rank9 = bst.rank(9);
        System.out.println("Rank of 3: " + rank3 + " (expected 2)");
        System.out.println("Rank of 5: " + rank5 + " (expected 3)");
        System.out.println("Rank of 9: " + rank9 + " (expected 5)");
        assert rank3 == 2 : "Expected rank 2 for key 3, got " + rank3;
        assert rank5 == 3 : "Expected rank 3 for key 5, got " + rank5;
        assert rank9 == 5 : "Expected rank 5 for key 9, got " + rank9;
        System.out.println("✓ Rank query working");
        
        // Test 4: Select query
        System.out.println("\nTest 4: Select query");
        Integer key2 = bst.select(2);
        Integer key3 = bst.select(3);
        Integer key5 = bst.select(5);
        System.out.println("2nd smallest: " + key2 + " (expected 3)");
        System.out.println("3rd smallest: " + key3 + " (expected 5)");
        System.out.println("5th smallest: " + key5 + " (expected 9)");
        assert key2 == 3 : "Expected 3, got " + key2;
        assert key3 == 5 : "Expected 5, got " + key3;
        assert key5 == 9 : "Expected 9, got " + key5;
        System.out.println("✓ Select query working");
        
        // Test 5: Delete and verify
        System.out.println("\nTest 5: Delete and verify");
        String removed = bst.remove(5);
        assert "five".equals(removed) : "Failed to remove 5";
        assert bst.get(5) == null : "Key 5 still exists after remove";
        size = bst.sizeSnapshot();
        System.out.println("Size after delete: " + size);
        assert size == 4 : "Expected size 4 after delete, got " + size;
        System.out.println("✓ Delete working");
        
        // Test 6: Rank/Select after delete
        System.out.println("\nTest 6: Rank/Select after delete");
        rank3 = bst.rank(3);
        rank9 = bst.rank(9);
        System.out.println("Rank of 3 after delete: " + rank3 + " (expected 2)");
        System.out.println("Rank of 9 after delete: " + rank9 + " (expected 4)");
        assert rank3 == 2 : "Expected rank 2 for key 3, got " + rank3;
        assert rank9 == 4 : "Expected rank 4 for key 9 after delete, got " + rank9;
        
        key3 = bst.select(3);
        System.out.println("3rd smallest after delete: " + key3 + " (expected 7)");
        assert key3 == 7 : "Expected 7, got " + key3;
        System.out.println("✓ Rank/Select after delete working");
        
        System.out.println("\n=== ALL TESTS PASSED ===");
        System.out.println(bst.getProfilingStats());
    }
}

