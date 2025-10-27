package bst;

import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;
import java.util.ArrayDeque;
import java.util.Objects;

public class MyBSTBaseline<K extends Comparable<? super K>, V> {
    //--------------------------------------------------------------------------------
    // Class: Node, LeafNode, InternalNode
    //--------------------------------------------------------------------------------
    protected static abstract class Node<E extends Comparable<? super E>, V> {
        final E key;
        volatile InternalNode<E,V> parent;
        Node(final E key) {
            this.key = key;
        }
    }

    protected static final class Version<E extends Comparable<? super E>> {
        final E key;
        final Version<E> left;
        final Version<E> right;
        final int nbChild;    // sum of keys if K is Integer; else 0

        Version(E key, Version<E> left, Version<E> right, int nbChild) {
            this.key = key;
            this.left = left;
            this.right = right;
            this.nbChild = nbChild;
        }
    }

    protected final static class LeafNode<E extends Comparable<? super E>, V> extends Node<E,V> {
        final V value;
        volatile Version<E> version;

        LeafNode(final E key, final V value) {
            super(key);
            this.value = value;
            int s = (key == null) ? 0 : 1;
            this.version = new Version<>(key, null, null, s);
        }
    }

    protected final static class InternalNode<E extends Comparable<? super E>, V> extends Node<E,V> {
        volatile Node<E,V> left;
        volatile Node<E,V> right;
        volatile Node<E,V> parent;
        volatile Info<E,V> info;
        volatile Version<E> version;


        InternalNode(final E key, final LeafNode<E,V> left, final LeafNode<E,V> right) {
            super(key);
            this.left = left;
            this.right = right;
            this.info = null;
            if (left  != null)  left.parent  = this;
            if (right != null)  right.parent = this;
            Version<E> vL = left.version;
            Version<E> vR = right.version;
            final int nb = (vL != null ? vL.nbChild : 0) + (vR != null ? vR.nbChild : 0);
            this.version = new Version<>(key, vL, vR, nb);
        }
    }

    protected static abstract class Info<E extends Comparable<? super E>, V> {
    }

    protected final static class DInfo<E extends Comparable<? super E>, V> extends Info<E,V> {
        final InternalNode<E,V> p;
        final LeafNode<E,V> l;
        final InternalNode<E,V> gp;
        final Info<E,V> pinfo;

        DInfo(final LeafNode<E,V> leaf, final InternalNode<E,V> parent, final InternalNode<E,V> grandparent, final Info<E,V> pinfo) {
            this.p = parent;
            this.l = leaf;
            this.gp = grandparent;
            this.pinfo = pinfo;
        }
    }

    protected final static class IInfo<E extends Comparable<? super E>, V> extends Info<E,V> {
        final InternalNode<E,V> p;
        final LeafNode<E,V> l;
        final Node<E,V> lReplacingNode;

        IInfo(final LeafNode<E,V> leaf, final InternalNode<E,V> parent, final Node<E,V> lReplacingNode){
            this.p = parent;
            this.l = leaf;
            this.lReplacingNode = lReplacingNode;
        }
    }

    protected final static class Mark<E extends Comparable<? super E>, V> extends Info<E,V> {
        final DInfo<E,V> dinfo;

        Mark(final DInfo<E,V> dinfo) {
            this.dinfo = dinfo;
        }
    }

    protected final static class Clean<E extends Comparable<? super E>, V> extends Info<E,V> {}

    //--------------------------------------------------------------------------------
// DICTIONARY
//--------------------------------------------------------------------------------
    private static final AtomicReferenceFieldUpdater<InternalNode, Node> leftUpdater = AtomicReferenceFieldUpdater.newUpdater(InternalNode.class, Node.class, "left");
    private static final AtomicReferenceFieldUpdater<InternalNode, Node> rightUpdater = AtomicReferenceFieldUpdater.newUpdater(InternalNode.class, Node.class, "right");
    private static final AtomicReferenceFieldUpdater<InternalNode, Info> infoUpdater = AtomicReferenceFieldUpdater.newUpdater(InternalNode.class, Info.class, "info");
    private static final AtomicReferenceFieldUpdater<InternalNode, Version> versionUpdater = AtomicReferenceFieldUpdater.newUpdater(InternalNode.class, Version.class, "version");


    final InternalNode<K,V> root;

    public MyBSTBaseline() {
        // to avoid handling special case when <= 2 nodes,
        // create 2 dummy nodes, both contain key null
        // All real keys inside BST are required to be non-null
        root = new InternalNode<K,V>(null, new LeafNode<K,V>(null, null), new LeafNode<K,V>(null, null));
    }

//--------------------------------------------------------------------------------
// PUBLIC METHODS:
// - find   : boolean
// - insert : boolean
// - delete : boolean
//--------------------------------------------------------------------------------

    /** PRECONDITION: key CANNOT BE NULL **/
    public final boolean containsKey(final K key) {
        return get(key) != null;
    }

    /** PRECONDITION: key CANNOT BE NULL **/
    public final V get(final K key) {
        if (key == null) throw new NullPointerException();
        Node<K,V> l = root.left;
        while (l.getClass() == InternalNode.class) {
            l = (l.key == null || key.compareTo(l.key) < 0) ? ((InternalNode<K,V>)l).left : ((InternalNode<K,V>)l).right;
        }
        return (l.key != null && key.compareTo(l.key) == 0) ? ((LeafNode<K,V>)l).value : null;
    }

    // Insert key to dictionary, returns the previous value associated with the specified key,
    // or null if there was no mapping for the key
    /** PRECONDITION: key, value CANNOT BE NULL **/
    public final V putIfAbsent(final K key, final V value){
        if (key == null || value == null) throw new NullPointerException();
        InternalNode<K,V> newInternal;
        LeafNode<K,V> newSibling, newNode;

        /** SEARCH VARIABLES **/
        InternalNode<K,V> p;
        Info<K,V> pinfo;
        Node<K,V> l;
        /** END SEARCH VARIABLES **/

        newNode = new LeafNode<K,V>(key, value);

        while (true) {

            /** SEARCH **/
            p = root;
            l = p.left;
            while (l.getClass() == InternalNode.class) {
                p = (InternalNode<K,V>)l;
                l = (p.key == null || key.compareTo(p.key) < 0) ? p.left : p.right;
            }
            pinfo = p.info;                             // read pinfo once instead of every iteration
            if (l != p.left && l != p.right) continue;  // then confirm the child link to l is valid
            // (just as if we'd read p's info field before the reference to l)
            /** END SEARCH **/

            LeafNode<K,V> foundLeaf = (LeafNode<K,V>)l;

            if (key.equals(foundLeaf.key)) {
                propagate(p);
                return foundLeaf.value; // key already in the tree, no duplicate allowed
            } else if (!(pinfo == null || pinfo.getClass() == Clean.class)) {
                help(pinfo);
            } else {
                newSibling = new LeafNode<K,V>(foundLeaf.key, foundLeaf.value);
                if (foundLeaf.key == null || key.compareTo(foundLeaf.key) < 0)  // newinternal = max(ret.foundLeaf.key, key);
                    newInternal = new InternalNode<K,V>(foundLeaf.key, newNode, newSibling);
                else
                    newInternal = new InternalNode<K,V>(key, newSibling, newNode);

                newInternal.parent = p;
                newSibling.parent = newInternal; newNode.parent = newInternal;

                final IInfo<K,V> newPInfo = new IInfo<K,V>(foundLeaf, p, newInternal);

                // try to IFlag parent
                if (infoUpdater.compareAndSet(p, pinfo, newPInfo)) {
                    helpInsert(newPInfo);
                    propagate(p);
                    return null;
                } else {
                    // if fails, help the current operation
                    // [CHECK]
                    // need to get the latest p.info since CAS doesnt return current value
                    help(p.info);
                }
            }
        }
    }

    // Insert key to dictionary, return the previous value associated with the specified key,
    // or null if there was no mapping for the key
    /** PRECONDITION: key, value CANNOT BE NULL **/
    public final V put(final K key, final V value) {
        if (key == null || value == null) throw new NullPointerException();
        InternalNode<K, V> newInternal;
        LeafNode<K, V> newSibling, newNode;
        IInfo<K, V> newPInfo;
        V result;

        /** SEARCH VARIABLES **/
        InternalNode<K, V> p;
        Info<K, V> pinfo;
        Node<K, V> l;
        /** END SEARCH VARIABLES **/
        newNode = new LeafNode<K, V>(key, value);

        while (true) {

            /** SEARCH **/
            p = root;
            l = p.left;
            while (l.getClass() == InternalNode.class) {
                p = (InternalNode<K,V>)l;
                l = (p.key == null || key.compareTo(p.key) < 0) ? p.left : p.right;
            }
            pinfo = p.info;                             // read pinfo once instead of every iteration
            if (l != p.left && l != p.right) continue;  // then confirm the child link to l is valid
            // (just as if we'd read p's info field before the reference to l)
            /** END SEARCH **/

            if (!(pinfo == null || pinfo.getClass() == Clean.class)) {
                help(pinfo);
            } else {
                LeafNode<K,V> foundLeaf = (LeafNode<K,V>)l;

                if (key.equals(foundLeaf.key)) {
                    // key already in the tree, try to replace the old node with new node
                    newPInfo = new IInfo<K, V>(foundLeaf, p, newNode);
                    propagate(p);
                    result = foundLeaf.value;
                } else {
                    // key is not in the tree, try to replace a leaf with a small subtree
                    newSibling = new LeafNode<K, V>(foundLeaf.key, foundLeaf.value);
                    if (foundLeaf.key == null || key.compareTo(foundLeaf.key) < 0) // newinternal = max(ret.foundLeaf.key, key);
                    {
                        newInternal = new InternalNode<K, V>(foundLeaf.key, newNode, newSibling);
                    } else {
                        newInternal = new InternalNode<K, V>(key, newSibling, newNode);
                    }
                    newInternal.parent = p;
                    newSibling.parent = newInternal; newNode.parent = newInternal;
                    newPInfo = new IInfo<K, V>(foundLeaf, p, newInternal);
                    result = null;
                }

                // try to IFlag parent
                if (infoUpdater.compareAndSet(p, pinfo, newPInfo)) {
                    helpInsert(newPInfo);
                    propagate(p);
                    return result;
                } else {
                    // if fails, help the current operation
                    // need to get the latest p.info since CAS doesnt return current value
                    help(p.info);
                }
            }
        }
    }

    // Delete key from dictionary, return the associated value when successful, null otherwise
    /** PRECONDITION: key CANNOT BE NULL **/
    public final V remove(final K key){
        if (key == null) throw new NullPointerException();

        /** SEARCH VARIABLES **/
        InternalNode<K,V> gp;
        Info<K,V> gpinfo;
        InternalNode<K,V> p;
        Info<K,V> pinfo;
        Node<K,V> l;
        /** END SEARCH VARIABLES **/

        while (true) {

            /** SEARCH **/
            gp = null;
            gpinfo = null;
            p = root;
            pinfo = p.info;
            l = p.left;
            while (l.getClass() == InternalNode.class) {
                gp = p;
                p = (InternalNode<K, V>) l;
                l = (p.key == null || key.compareTo(p.key) < 0) ? p.left : p.right;
            }
            // note: gp can be null here, because clearly the root.left.left == null
            //       when the tree is empty. however, in this case, l.key will be null,
            //       and the function will return null, so this does not pose a problem.
            if (gp != null) {
                gpinfo = gp.info;                               // - read gpinfo once instead of every iteration
                if (p != gp.left && p != gp.right) continue;    //   then confirm the child link to p is valid
                pinfo = p.info;                                 //   (just as if we'd read gp's info field before the reference to p)
                if (l != p.left && l != p.right) continue;      // - do the same for pinfo and l
            }
            /** END SEARCH **/

            if (!key.equals(l.key)) {
                propagate(p); return null;
            }else if (!(gpinfo == null || gpinfo.getClass() == Clean.class)) {
                help(gpinfo);
            } else if (!(pinfo == null || pinfo.getClass() == Clean.class)) {
                help(pinfo);
            } else {
                LeafNode<K,V> foundLeaf = (LeafNode<K,V>)l;
                // try to DFlag grandparent
                final DInfo<K,V> newGPInfo = new DInfo<K,V>(foundLeaf, p, gp, pinfo);

                if (infoUpdater.compareAndSet(gp, gpinfo, newGPInfo)) {
                    if (helpDelete(newGPInfo)) return foundLeaf.value;
                } else {
                    // if fails, help grandparent with its latest info value
                    help(gp.info);
                }
            }
        }
    }

//--------------------------------------------------------------------------------
// PRIVATE METHODS
// - helpInsert
// - helpDelete
//--------------------------------------------------------------------------------

    private void helpInsert(final IInfo<K,V> info){
        boolean onLeft = (info.p.left == info.l);
        boolean spliced = onLeft
                ? leftUpdater.compareAndSet(info.p, info.l, info.lReplacingNode)
                : rightUpdater.compareAndSet(info.p, info.l, info.lReplacingNode);

        if (spliced) {
            // Fix parent of the new child
            info.lReplacingNode.parent = info.p;
            // NEW: ensure snapshot is refreshed even if a helper did the splice
            propagate(info.p);
        }
        infoUpdater.compareAndSet(info.p, info, new Clean());
    }

    private boolean helpDelete(final DInfo<K,V> info){
        final boolean result;

        result = infoUpdater.compareAndSet(info.p, info.pinfo, new Mark<K,V>(info));
        final Info<K,V> currentPInfo = info.p.info;
        if (result || (currentPInfo.getClass() == Mark.class && ((Mark<K,V>) currentPInfo).dinfo == info)) {
            // CAS succeeded or somebody else already helped
            helpMarked(info);
            return true;
        } else {
            help(currentPInfo);
            infoUpdater.compareAndSet(info.gp, info, new Clean());
            return false;
        }
    }

    private void help(final Info<K,V> info) {
        if (info.getClass() == IInfo.class)     helpInsert((IInfo<K,V>) info);
        else if(info.getClass() == DInfo.class) helpDelete((DInfo<K,V>) info);
        else if(info.getClass() == Mark.class)  helpMarked(((Mark<K,V>)info).dinfo);
    }

    private void helpMarked(final DInfo<K,V> info) {
        final Node<K,V> other = (info.p.right == info.l) ? info.p.left : info.p.right;
        boolean pIsLeft = (info.gp.left == info.p);
        boolean swung = pIsLeft
                ? leftUpdater.compareAndSet(info.gp, info.p, other)
                : rightUpdater.compareAndSet(info.gp, info.p, other);

        if (swung) {
            // Fix parent of the moved-up child
            other.parent = info.gp;
            // Keep snapshot fresh when a helper completes the swing
            propagate(info.gp);
        }
        infoUpdater.compareAndSet(info.gp, info, new Clean<>());
    }

    private static <E extends Comparable<? super E>, T> Version<E> childVersion(Node<E,T> n) {
        if (n instanceof InternalNode) return ((InternalNode<E,T>) n).version;
        return ((LeafNode<E,T>) n).version;
    }

    private static <E extends Comparable<? super E>, T> boolean refresh(InternalNode<E,T> x) {
        // snapshot old
        final Version<E> old = x.version;

        // Read left consistently: (ptr, then version derived from same ptr, recheck ptr)
        Node<E,T> xL; Version<E> vL;
        do {
            xL = x.left;
            vL = (xL instanceof InternalNode) ? ((InternalNode<E,T>) xL).version : ((LeafNode<E,T>) xL).version;
        } while (x.left != xL);

        // Read right consistently
        Node<E,T> xR; Version<E> vR;
        do {
            xR = x.right;
            vR = (xR instanceof InternalNode) ? ((InternalNode<E,T>) xR).version : ((LeafNode<E,T>) xR).version;
        } while (x.right != xR);
        int nb = vL.nbChild + vR.nbChild;
        Version<E> newer = new Version<>(x.key, vL, vR, nb);
        return versionUpdater.compareAndSet(x, old, newer);
    }

    private static <E extends Comparable<? super E>, T> void propagate(Node<E,T> start) {
        Node<E,T> x = start;
        int tries = 0;
        while (x != null) {
            if (x instanceof InternalNode) {
                if (!refresh((InternalNode<E,T>) x)) {
                    refresh((InternalNode<E,T>) x);
                }
            }
            x = x.parent;
        }
    }

    /**
     *
     * DEBUG CODE (FOR TESTBED)
     *
     */

    public long getSumOfKeys() {
        return getSumOfKeys(root);
    }

    private long getSumOfKeys(Node node) {
        long sum = 0;
        if (node.getClass() == LeafNode.class)
            sum += node.key != null ? (int) (Integer) node.key : 0;
        else
            sum += getSumOfKeys(((InternalNode<K,V>)node).left) + getSumOfKeys(((InternalNode<K,V>)node).right);
        return sum;
    }

    public int sizeStructural() {
        return sizeStructural(root);
    }
    private int sizeStructural(Node<K,V> n) {
        if (n instanceof LeafNode) return (((LeafNode<K,V>) n).key != null) ? 1 : 0;
        InternalNode<K,V> i = (InternalNode<K,V>) n;
        return sizeStructural(i.left) + sizeStructural(i.right);
    }

    public int sizeSnapshot() {
        Version<K> v = root.version;
        return (v != null) ? v.nbChild : 0;
    }

    public boolean containsKeySnapshot(K key) {
        Objects.requireNonNull(key, "key");
        Version<K> v = root.version;
        if (v == null) return false;
        while (v.left != null) {
            if (v.key == null || key.compareTo(v.key) < 0) v = v.left;
            else v = v.right;
        }
        return (v.key != null) && (key.compareTo(v.key) == 0);
    }

    public K selectKth(int j) {
        Version<K> v = root.version;
        if (v == null || j <= 0 || j > v.nbChild) return null;
        while (v.left != null) {
            int leftSize = (v.left != null) ? v.left.nbChild : 0;
            if (j <= leftSize) v = v.left;
            else { j -= leftSize; v = v.right; }
        }
        return v.key;
    }
}

