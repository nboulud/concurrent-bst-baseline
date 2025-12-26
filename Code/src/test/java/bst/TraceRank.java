package bst;
import bst.MyBSTnext;

public class TraceRank {
    public static void main(String[] args) {
        MyBSTnext<Integer, String> tree = new MyBSTnext<>();
        
        tree.putIfAbsent(5, "five");
        tree.putIfAbsent(3, "three");
        tree.putIfAbsent(7, "seven");
        
        System.out.println("=== Tree structure (BST) ===");
        System.out.println("Size: " + tree.sizeStructural());
        System.out.println("Sum of keys: " + tree.getSumOfKeys());
        System.out.println();
        
        // Test get to make sure keys are there
        System.out.println("get(3) = " + tree.get(3));
        System.out.println("get(5) = " + tree.get(5));
        System.out.println("get(7) = " + tree.get(7));
        System.out.println();
        
        System.out.println("Testing rank(5):");
        int rank5 = tree.rank(5);
        System.out.println("Result: " + rank5 + " (expected 2)");
    }
}
