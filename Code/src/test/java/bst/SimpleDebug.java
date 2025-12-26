package bst;
import bst.MyBSTnext;

public class SimpleDebug {
    public static void main(String[] args) {
        MyBSTnext<Integer, String> tree = new MyBSTnext<>();
        
        tree.putIfAbsent(5, "five");
        tree.putIfAbsent(3, "three");
        tree.putIfAbsent(7, "seven");
        
        System.out.println("Size: " + tree.sizeSnapshot());
        System.out.println("Size structural: " + tree.sizeStructural());
        System.out.println("Rank of 5: " + tree.rank(5));
    }
}
