package bst;

import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;
import java.util.concurrent.atomic.AtomicLong;
import java.util.Objects;

public class MyBST<K extends Comparable<? super K>, V> {
    //--------------------------------------------------------------------------------
    // HANDSHAKE CONSTANTS AND FIELDS
    //--------------------------------------------------------------------------------
    private static final int IDLE_PHASE = -1;
    private static final int FAST_PHASE = -2;
    private static final int MAX_THREADS = 256;  // Maximum number of threads
    
    private final AtomicLong sizePhase;  // Global phase counter for handshakes
    private final AtomicLong[] opPhase;  // Per-thread phase array
    private final AtomicLong[] fastMetadataCounters;  // Per-thread fast path size counters
    private final AtomicLong maxThreadID;  // Track highest thread ID seen (for optimization)
    
    // Debug/profiling counters
    public final AtomicLong totalHandshakes = new AtomicLong(0);
    public final AtomicLong totalHandshakeTimeNanos = new AtomicLong(0);
    public final AtomicLong totalSizeCalls = new AtomicLong(0);
    
    // ThreadLocal for stable thread IDs
    private static final ThreadLocal<Integer> threadID = ThreadLocal.withInitial(() -> {
        return ThreadIDAllocator.allocate();
    });
    
    // Simple thread ID allocator
    private static class ThreadIDAllocator {
        private static final AtomicLong counter = new AtomicLong(0);
        static int allocate() {
            return (int) counter.getAndIncrement();
        }
    }
    
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
        final int nbChild;    // slow path metadata: count of nodes in subtree
        final Node<E,?> node; // reference back to node for accessing fast path metadata

        Version(E key, Version<E> left, Version<E> right, int nbChild, Node<E,?> node) {
            this.key = key;
            this.left = left;
            this.right = right;
            this.nbChild = nbChild;
            this.node = node;
        }
    }

    protected final static class LeafNode<E extends Comparable<? super E>, V> extends Node<E,V> {
        final V value;
        volatile Version<E> version;

        LeafNode(final E key, final V value) {
            super(key);
            this.value = value;
            // Start with nbChild=0; will be updated via propagate() in slow path only
            this.version = new Version<>(key, null, null, 0, this);
        }
    }

    protected final static class InternalNode<E extends Comparable<? super E>, V> extends Node<E,V> {
        volatile Node<E,V> left;
        volatile Node<E,V> right;
        volatile Node<E,V> parent;
        volatile Info<E,V> info;
        volatile Version<E> version;
        final AtomicLong fastSize;  // Fast path metadata for size


        InternalNode(final E key, final LeafNode<E,V> left, final LeafNode<E,V> right) {
            super(key);
            this.left = left;
            this.right = right;
            this.info = null;
            if (left  != null)  left.parent  = this;
            if (right != null)  right.parent = this;
            Version<E> vL = left.version;
            Version<E> vR = right.version;
            // Version tree starts at 0, only updated via propagate() (slow path)
            this.fastSize = new AtomicLong(0);  // Not currently used
            this.version = new Version<>(key, vL, vR, 0, this);
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

    public MyBST() {
        // Initialize handshake infrastructure
        this.sizePhase = new AtomicLong(4);  // Start at 4 (divisible by 4 means fast path)
        this.opPhase = new AtomicLong[MAX_THREADS];
        this.fastMetadataCounters = new AtomicLong[MAX_THREADS];
        this.maxThreadID = new AtomicLong(-1);  // Track highest thread ID for optimization
        for (int i = 0; i < MAX_THREADS; i++) {
            this.opPhase[i] = new AtomicLong(IDLE_PHASE);
            this.fastMetadataCounters[i] = new AtomicLong(0);
        }
        
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
        
        // Announce we're starting an operation
        long phase = getSizePhase();
        boolean useFastPath = (phase % 4 == 0);
        setOpPhaseVolatile(useFastPath ? FAST_PHASE : phase);
        
        try {
            InternalNode<K,V> newInternal;
            LeafNode<K,V> newSibling, newNode;

            /** SEARCH VARIABLES **/
            InternalNode<K,V> p;
            Info<K,V> pinfo;
            Node<K,V> l;
            /** END SEARCH VARIABLES **/

            newNode = new LeafNode<K,V>(key, value);

            while (true) {
                // Re-check phase on every retry to respond quickly to handshakes
                long newPhase = getSizePhase();
                boolean newFastPath = (newPhase % 4 == 0);
                if (newFastPath != useFastPath) {
                    useFastPath = newFastPath;
                    setOpPhaseVolatile(useFastPath ? FAST_PHASE : newPhase);
                }

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
                    if (!useFastPath) propagate(p);
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
                        helpInsert(newPInfo, useFastPath);
                        
                        // Update metadata - successful insert
                        if (useFastPath) {
                            fastUpdateMetadata(1);  // Fast path: simple atomic increment
                        } else {
                            propagate(p);  // Slow path: full propagation
                        }
                        
                        return null;
                    } else {
                        // if fails, help the current operation
                        // [CHECK]
                        // need to get the latest p.info since CAS doesnt return current value
                        help(p.info);
                    }
                }
            }
        } finally {
            // Return to idle phase
            setOpPhaseIdle();
        }
    }

    // Insert key to dictionary, return the previous value associated with the specified key,
    // or null if there was no mapping for the key
    /** PRECONDITION: key, value CANNOT BE NULL **/
    public final V put(final K key, final V value) {
        if (key == null || value == null) throw new NullPointerException();
        
        // Announce we're starting an operation
        long phase = getSizePhase();
        boolean useFastPath = (phase % 4 == 0);
        setOpPhaseVolatile(useFastPath ? FAST_PHASE : phase);
        
        try {
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
                // Re-check phase on every retry to respond quickly to handshakes
                long newPhase = getSizePhase();
                boolean newFastPath = (newPhase % 4 == 0);
                if (newFastPath != useFastPath) {
                    useFastPath = newFastPath;
                    setOpPhaseVolatile(useFastPath ? FAST_PHASE : newPhase);
                }

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
                        if (!useFastPath) propagate(p);
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
                        helpInsert(newPInfo, useFastPath);
                        
                        // Update metadata based on path
                        if (result == null) {  // Successful insert
                            if (useFastPath) {
                                fastUpdateMetadata(1);  // Fast path: simple atomic increment
                            } else {
                                propagate(p);  // Slow path: full propagation
                            }
                        }
                        
                        return result;
                    } else {
                        // if fails, help the current operation
                        // need to get the latest p.info since CAS doesnt return current value
                        help(p.info);
                    }
                }
            }
        } finally {
            // Return to idle phase
            setOpPhaseIdle();
        }
    }

    // Delete key from dictionary, return the associated value when successful, null otherwise
    /** PRECONDITION: key CANNOT BE NULL **/
    public final V remove(final K key){
        if (key == null) throw new NullPointerException();
        
        // Announce we're starting an operation
        long phase = getSizePhase();
        boolean useFastPath = (phase % 4 == 0);
        setOpPhaseVolatile(useFastPath ? FAST_PHASE : phase);
        
        try {
            /** SEARCH VARIABLES **/
            InternalNode<K,V> gp;
            Info<K,V> gpinfo;
            InternalNode<K,V> p;
            Info<K,V> pinfo;
            Node<K,V> l;
            /** END SEARCH VARIABLES **/

            while (true) {
                // Re-check phase on every retry to respond quickly to handshakes
                long newPhase = getSizePhase();
                boolean newFastPath = (newPhase % 4 == 0);
                if (newFastPath != useFastPath) {
                    useFastPath = newFastPath;
                    setOpPhaseVolatile(useFastPath ? FAST_PHASE : newPhase);
                }

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
                    if (!useFastPath) propagate(p);
                    return null;
                }else if (!(gpinfo == null || gpinfo.getClass() == Clean.class)) {
                    help(gpinfo);
                } else if (!(pinfo == null || pinfo.getClass() == Clean.class)) {
                    help(pinfo);
                } else {
                    LeafNode<K,V> foundLeaf = (LeafNode<K,V>)l;
                    // try to DFlag grandparent
                    final DInfo<K,V> newGPInfo = new DInfo<K,V>(foundLeaf, p, gp, pinfo);

                    if (infoUpdater.compareAndSet(gp, gpinfo, newGPInfo)) {
                        if (helpDelete(newGPInfo, useFastPath)) {
                            // Update metadata based on path
                            if (useFastPath) {
                                fastUpdateMetadata(-1);  // Fast path: simple atomic decrement
                            } else {
                                propagate(gp);  // Slow path: full propagation
                            }
                            return foundLeaf.value;
                        }
                    } else {
                        // if fails, help grandparent with its latest info value
                        help(gp.info);
                    }
                }
            }
        } finally {
            // Return to idle phase
            setOpPhaseIdle();
        }
    }

//--------------------------------------------------------------------------------
// PRIVATE METHODS
// - helpInsert
// - helpDelete
//--------------------------------------------------------------------------------

    private void helpInsert(final IInfo<K,V> info, boolean useFastPath){
        boolean onLeft = (info.p.left == info.l);
        boolean spliced = onLeft
                ? leftUpdater.compareAndSet(info.p, info.l, info.lReplacingNode)
                : rightUpdater.compareAndSet(info.p, info.l, info.lReplacingNode);

        if (spliced) {
            // Fix parent of the new child
            info.lReplacingNode.parent = info.p;
            // Only propagate if slow path
            if (!useFastPath) {
                propagate(info.p);
            }
        }
        infoUpdater.compareAndSet(info.p, info, new Clean());
    }

    private boolean helpDelete(final DInfo<K,V> info, boolean useFastPath){
        final boolean result;

        result = infoUpdater.compareAndSet(info.p, info.pinfo, new Mark<K,V>(info));
        final Info<K,V> currentPInfo = info.p.info;
        if (result || (currentPInfo.getClass() == Mark.class && ((Mark<K,V>) currentPInfo).dinfo == info)) {
            // CAS succeeded or somebody else already helped
            helpMarked(info, useFastPath);
            return true;
        } else {
            help(currentPInfo);
            infoUpdater.compareAndSet(info.gp, info, new Clean());
            return false;
        }
    }

    private void help(final Info<K,V> info) {
        // When helping, use slow path to be conservative
        if (info.getClass() == IInfo.class)     helpInsert((IInfo<K,V>) info, false);
        else if(info.getClass() == DInfo.class) helpDelete((DInfo<K,V>) info, false);
        else if(info.getClass() == Mark.class)  helpMarked(((Mark<K,V>)info).dinfo, false);
    }

    private void helpMarked(final DInfo<K,V> info, boolean useFastPath) {
        final Node<K,V> other = (info.p.right == info.l) ? info.p.left : info.p.right;
        boolean pIsLeft = (info.gp.left == info.p);
        boolean swung = pIsLeft
                ? leftUpdater.compareAndSet(info.gp, info.p, other)
                : rightUpdater.compareAndSet(info.gp, info.p, other);

        if (swung) {
            // Fix parent of the moved-up child
            other.parent = info.gp;
            // Only propagate if slow path
            if (!useFastPath) {
                propagate(info.gp);
            }
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
        Version<E> newer = new Version<>(x.key, vL, vR, nb, x);  // Pass node reference
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

    //--------------------------------------------------------------------------------
    // HANDSHAKE MECHANISM
    //--------------------------------------------------------------------------------
    
    private void setOpPhaseIdle() {
        int tid = threadID.get();
        if (tid < MAX_THREADS) {
            opPhase[tid].set(IDLE_PHASE);
        }
    }
    
    private void setOpPhaseVolatile(long phase) {
        int tid = threadID.get();
        if (tid < MAX_THREADS) {
            // Update max thread ID if this is a new thread
            long currentMax;
            do {
                currentMax = maxThreadID.get();
                if (tid <= currentMax) break;
            } while (!maxThreadID.compareAndSet(currentMax, tid));
            
            opPhase[tid].set(phase);
        }
    }
    
    private long getSizePhase() {
        return sizePhase.get();
    }
    
    private void performHandshake(long targetPhase) {
        long startTime = System.nanoTime();
        
        // Only check threads that have been active (optimization)
        int activeThreads = (int)maxThreadID.get() + 1;
        if (activeThreads <= 0) return;  // No threads have started yet
        
        for (int tid = 0; tid < activeThreads; tid++) {
            long phase;
            while ((phase = opPhase[tid].get()) != IDLE_PHASE && phase < targetPhase) {
                // Spin wait for thread to acknowledge handshake
                Thread.onSpinWait();
            }
        }
        
        long elapsed = System.nanoTime() - startTime;
        totalHandshakes.incrementAndGet();
        totalHandshakeTimeNanos.addAndGet(elapsed);
    }
    
    private long doFirstAndSecondHandshakes() {
        // Wait until sizePhase % 4 == 0 (fast path mode)
        long currSizePhase;
        do {
            currSizePhase = sizePhase.get();
        } while (currSizePhase % 4 != 0);
        
        // First handshake: increment by 1
        sizePhase.set(currSizePhase + 1);
        performHandshake(currSizePhase + 1);
        
        // Second handshake: increment by 1 again
        sizePhase.set(currSizePhase + 2);
        performHandshake(currSizePhase + 2);
        
        return currSizePhase + 2;
    }
    
    private long computeFastSize() {
        // Only sum active threads (optimization)
        int activeThreads = (int)maxThreadID.get() + 1;
        if (activeThreads <= 0) return 0;
        
        long fastSize = 0;
        for (int tid = 0; tid < activeThreads; tid++) {
            fastSize += fastMetadataCounters[tid].get();
        }
        return fastSize;
    }
    
    private void fastUpdateMetadata(int delta) {
        int tid = threadID.get();
        if (tid < MAX_THREADS) {
            fastMetadataCounters[tid].addAndGet(delta);
        }
    }
    
    //--------------------------------------------------------------------------------
    // FAST AND SLOW PATH OPERATIONS
    //--------------------------------------------------------------------------------
    
    private boolean isSlowPathActive() {
        long phase = getSizePhase();
        return phase % 4 != 0;  // Not in fast path if phase % 4 != 0
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
        totalSizeCalls.incrementAndGet();
        
        // Perform two handshakes
        long currSizePhase = doFirstAndSecondHandshakes();
        
        // Compute fast path size (sum of per-thread counters)
        long fastSize = computeFastSize();
        
        // Compute slow path size (from Version tree)
        Version<K> v = root.version;
        int slowSize = (v != null) ? v.nbChild : 0;
        
        // Signal completion: increment sizePhase by 2
        sizePhase.set(currSizePhase + 2);
        
        // Return combined size
        return (int)(fastSize + slowSize);
    }
    
    // Get profiling statistics
    public String getProfilingStats() {
        long handshakes = totalHandshakes.get();
        long totalTimeNanos = totalHandshakeTimeNanos.get();
        long sizeCalls = totalSizeCalls.get();
        
        double avgHandshakeUs = handshakes > 0 ? (totalTimeNanos / (double)handshakes / 1000.0) : 0;
        double handshakesPerSize = sizeCalls > 0 ? (handshakes / (double)sizeCalls) : 0;
        
        return String.format("Profiling: %d size calls, %d handshakes (%.1f per size), avg handshake time: %.2f μs",
            sizeCalls, handshakes, handshakesPerSize, avgHandshakeUs);
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
