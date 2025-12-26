package bst;

import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;
import java.util.concurrent.atomic.AtomicReferenceArray;
import java.util.concurrent.atomic.AtomicLong;
import java.util.Objects;

public class MyBSTnext<K extends Comparable<? super K>, V> {
    
    private static final int IDLE_PHASE = -1;
    private static final int FAST_PHASE = -2;
    private static final int MAX_THREADS = 256;  
    
    private final AtomicReferenceArray<AtomicLong> opPhase; // opPhase for each thread
    private final AtomicLong queriesPhase; // Global synchronization for query operations (size, rank, select)
    private final AtomicLong maxThreadID;  // Track highest thread ID seen (opt)
    private final AtomicLong activeReaders;  // Count of active aggregate queries in slow path
    
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
        final int nbChild;    
        final Node<E,?> node; 

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
        final AtomicLong fastSize;  // Fast path metadata for size
        volatile Version<E> forwardingPtr;  // Points to replacement Version (can be orphaned during insert)
        volatile Node<E,V> reversePtr;  // Points to predecessor (for chain compression)

        LeafNode(final E key, final V value) {
            super(key);
            this.value = value;
            // Start with fastSize=1 if key is not null (real element), 0 for sentinel
            this.fastSize = new AtomicLong((key != null) ? 1 : 0);
            // Start with nbChild=0; will be updated via propagate() in slow path only
            this.version = new Version<>(key, null, null, 0, this);
            // Initialize forwarding and reverse pointers to null
            this.forwardingPtr = null;
            this.reversePtr = null;
        }
    }

    protected final static class InternalNode<E extends Comparable<? super E>, V> extends Node<E,V> {
        volatile Node<E,V> left;
        volatile Node<E,V> right;
        volatile Node<E,V> parent;
        volatile Info<E,V> info;
        volatile Version<E> version;
        final AtomicLong fastSize;  // Fast path metadata for size
        volatile Version<E> forwardingPtr;  // Points to replacement Version (for query navigation)
        volatile Node<E,V> reversePtr;  // Points to predecessor (for chain compression), can be leaf or internal


        InternalNode(final E key, final LeafNode<E,V> left, final LeafNode<E,V> right) {
            super(key);
            this.left = left;
            this.right = right;
            this.info = null;
            if (left  != null)  left.parent  = this;
            if (right != null)  right.parent = this;
            Version<E> vL = left.version;
            Version<E> vR = right.version;
            // Initialize fastSize from children's fastSize
            long initialFastSize = 0;
            if (left != null) initialFastSize += left.fastSize.get();
            if (right != null) initialFastSize += right.fastSize.get();
            this.fastSize = new AtomicLong(initialFastSize);
            // Version tree starts at 0, only updated via propagate() (slow path)
            this.version = new Version<>(key, vL, vR, 0, this);
            // Initialize forwarding and reverse pointers to null
            this.forwardingPtr = null;
            this.reversePtr = null;
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

    public MyBSTnext() {
        // Initialize handshake infrastructure
        this.queriesPhase = new AtomicLong(0);  // Start at 0 (mod 4 = 0 means fast path)
        this.opPhase = new AtomicReferenceArray<>(MAX_THREADS);
        this.maxThreadID = new AtomicLong(-1);  // Track highest thread ID for optimization
        this.activeReaders = new AtomicLong(0);  // No active aggregate queries initially
        for (int i = 0; i < MAX_THREADS; i++) {
            this.opPhase.set(i, new AtomicLong(IDLE_PHASE));
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
    /** 
     * Get with phase-based logic and slow operation dependency handling.
     * PRECONDITION: key CANNOT BE NULL 
     */
    public final V get(final K key) {
        if (key == null) throw new NullPointerException();
        
        long currentPhase = queriesPhase.get();
        
        // Phase 2: Use Version tree navigation (slow contains)
        if ((currentPhase & 3) == 2) {
            // Navigate Version tree to find key
            V result = getViaVersionTree(key);
            
            // After getting the result, check if we are still in a slow phase.
            long exitPhase = queriesPhase.get();
            if ((exitPhase & 3) == 2) {
                // Still in a slow phase - result is valid
                return result;
            } else {
                // Phase changed to fast/handshake - retry with current phase
                return get(key);
            }
        }
        
        // Phase 0 or 1: Use BST navigation (fast contains)
        Node<K,V> l = root.left;
        while (l.getClass() == InternalNode.class) {
            l = (l.key == null || key.compareTo(l.key) < 0) ? ((InternalNode<K,V>)l).left : ((InternalNode<K,V>)l).right;
        }
        
        // If we started in Phase 1, check if we transitioned to slow path during the operation
        if ((currentPhase & 3) == 1 ){
            if ((queriesPhase.get() & 3) == 2) {
                return get(key); // Retry
            }else{
                if (l.key != null && key.compareTo(l.key) == 0) {
                    return ((LeafNode<K,V>)l).value;
                }     // Still in fast path - return result
            }
        }else{
            if (l.key != null && key.compareTo(l.key) == 0) {
                return ((LeafNode<K,V>)l).value;
            }     // Still in fast path - return result
        }
        
        return null;  // Key not found
    }
    
    /**
     * Helper for contains using Version tree navigation (used in Phase 2).
     */
    private V getViaVersionTree(K key) {
        Version<K> v = root.version;
        if (v == null) return null;
        
        while (v.left != null) {
            if (v.key == null || key.compareTo(v.key) < 0) {
                v = v.left;
            } else {
                v = v.right;
            }
        }
        
        if (v.key != null && key.compareTo(v.key) == 0) {
            return ((LeafNode<K,V>) v.node).value;
        }
        return null;
    }

    // Insert key to dictionary, returns the previous value associated with the specified key,
    // or null if there was no mapping for the key
    /** PRECONDITION: key, value CANNOT BE NULL **/
    public final V putIfAbsent(final K key, final V value){
        if (key == null || value == null) throw new NullPointerException();
        
        // Announce FAST_PHASE immediately (optimistic), then check if we need to correct it
        setOpPhaseVolatile(FAST_PHASE);
        long currentQueriesPhase = getQueriesPhase();
        boolean useFastPath = ((currentQueriesPhase & 3) == 0);
        if (!useFastPath) {
            // A query operation is in progress, switch to slow path
            setOpPhaseVolatile(currentQueriesPhase);
        }
        
        try {
            InternalNode<K,V> newInternal;
            LeafNode<K,V> newSibling, newNode;

            //Search varaiables 
            InternalNode<K,V> p;
            Info<K,V> pinfo;
            Node<K,V> l;

            newNode = new LeafNode<K,V>(key, value);

            while (true) {
                // Re-check phase on every retry to respond quickly to handshakes
                long newPhase = getQueriesPhase();
                boolean newFastPath = ((newPhase & 3) == 0);
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
                    if (!useFastPath) {
                        propagate(p);  // Slow path: update version tree with counter tracking
                    }
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
                            fastUpdateMetadataOnly(1, p);  // Fast path: update fastSize only
                            // Version structure already updated in helpInsert
                        } else {
                            propagate(p);  // Slow path: full propagation with counter tracking
                        }
                        
                        return null;
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

    // Insert key to dictionary, return the previous value associated with the specified key,
    // or null if there was no mapping for the key
    /** PRECONDITION: key, value CANNOT BE NULL **/
    public final V put(final K key, final V value) {
        if (key == null || value == null) throw new NullPointerException();
        
        // Announce FAST_PHASE immediately (optimistic), then check if we need to correct it
        setOpPhaseVolatile(FAST_PHASE);
        long currentQueriesPhase = getQueriesPhase();
        boolean useFastPath = ((currentQueriesPhase & 3) == 0);
        if (!useFastPath) {
            // A query operation is in progress, switch to slow path
            setOpPhaseVolatile(currentQueriesPhase);
        }
        
        try {
            InternalNode<K, V> newInternal;
            LeafNode<K, V> newSibling, newNode;
            IInfo<K, V> newPInfo;
            V result;

            //Search varaiables 
            InternalNode<K, V> p;
            Info<K, V> pinfo;
            Node<K, V> l;

            newNode = new LeafNode<K, V>(key, value);

            while (true) {
                // Re-check phase on every retry to respond quickly to handshakes
                long newPhase = getQueriesPhase();
                boolean newFastPath = ((newPhase & 3) == 0);
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
                        if (result == null) {  // Successful insert (newInternal was created)
                            if (useFastPath) {
                                // Start from parent since we just added newInternal below it
                                fastUpdateMetadataOnly(1, p);  // Fast path: update fastSize only
                                // Version structure already updated in helpInsert
                            } else {
                                propagate(p);  // Slow path: full propagation with counter tracking
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
        
        // Announce FAST_PHASE immediately (optimistic), then check if we need to correct it
        setOpPhaseVolatile(FAST_PHASE);
        long currentQueriesPhase = getQueriesPhase();
        boolean useFastPath = ((currentQueriesPhase & 3) == 0);
        if (!useFastPath) {
            // A query operation is in progress, switch to slow path
            setOpPhaseVolatile(currentQueriesPhase);
        }
        
        try {
            
            //Search varaiables 
            InternalNode<K,V> gp;
            Info<K,V> gpinfo;
            InternalNode<K,V> p;
            Info<K,V> pinfo;
            Node<K,V> l;
            

            while (true) {
                // Re-check phase on every retry to respond quickly to handshakes
                long newPhase = getQueriesPhase();
                boolean newFastPath = ((newPhase & 3) == 0);
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
                                fastUpdateMetadataOnly(-1, gp);  // Fast path: update fastSize only
                                // Version structure already updated in helpMarked
                            } else {
                                propagate(gp);  // Slow path: full propagation with counter tracking
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
            
            if (useFastPath) {
                // Fast path with chain compression
                // Determine final target version (follow existing forward chain if present)
                Version<K> replacementVersion = (info.lReplacingNode instanceof InternalNode) 
                    ? ((InternalNode<K,V>) info.lReplacingNode).version
                    : ((LeafNode<K,V>) info.lReplacingNode).version;
                
                Version<K> finalTarget = replacementVersion;
                
                // Check if replacement already points forward (upward compression)
                if (info.lReplacingNode instanceof InternalNode) {
                    InternalNode<K,V> replacement = (InternalNode<K,V>) info.lReplacingNode;
                    if (replacement.forwardingPtr != null) {
                        finalTarget = replacement.forwardingPtr;
                    }
                    
                    // Check if we're extending an existing chain (downward compression)
                    if (replacement.reversePtr != null) {
                        // Update predecessor to skip intermediate nodes
                        Node<K,V> predecessor = replacement.reversePtr;
                        // Set forwardingPtr on predecessor
                        if (predecessor instanceof InternalNode) {
                            ((InternalNode<K,V>) predecessor).forwardingPtr = finalTarget;
                        } else if (predecessor instanceof LeafNode) {
                            ((LeafNode<K,V>) predecessor).forwardingPtr = finalTarget;
                        }
                        // reversePtr stays at predecessor - don't update
                    } else {
                        // First link - create new chain
                        // Set forwardingPtr on orphaned leaf (info.l is always LeafNode in insert)
                        info.l.forwardingPtr = finalTarget;
                        // Set reverse pointer on replacement (points back to info.l)
                        if (finalTarget.node instanceof InternalNode) {
                            ((InternalNode<K,V>) finalTarget.node).reversePtr = info.l;
                        } else if (finalTarget.node instanceof LeafNode) {
                            ((LeafNode<K,V>) finalTarget.node).reversePtr = info.l;
                        }
                    }
                } else {
                    // Replacement is a leaf - create simple forwarding link
                    info.l.forwardingPtr = finalTarget;
                    // Set reverse pointer on replacement leaf
                    if (finalTarget.node instanceof LeafNode) {
                        ((LeafNode<K,V>) finalTarget.node).reversePtr = info.l;
                    }
                }
                // Metadata updates happen in caller via fastUpdateMetadataOnly
            } else {
                // Slow path: Full propagation (updates both structure and nbChild)
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
            
            if (useFastPath) {
                // Fast path with chain compression
                // Determine final target (follow existing forward chain if present)
                Version<K> finalTarget = null;
                if (other instanceof InternalNode) {
                    InternalNode<K,V> otherInternal = (InternalNode<K,V>) other;
                    finalTarget = otherInternal.version;
                    
                    // Check if 'other' already points forward (upward compression)
                    if (otherInternal.forwardingPtr != null) {
                        finalTarget = otherInternal.forwardingPtr;
                    }
                } else {
                    LeafNode<K,V> otherLeaf = (LeafNode<K,V>) other;
                    finalTarget = otherLeaf.version;
                    
                    // Check if 'other' (leaf) already points forward (upward compression)
                    if (otherLeaf.forwardingPtr != null) {
                        finalTarget = otherLeaf.forwardingPtr;
                    }
                }
                
                // Check if orphaned node (info.p) has a predecessor (downward compression)
                if (info.p.reversePtr != null) {
                    // Update predecessor to skip info.p
                    Node<K,V> predecessor = info.p.reversePtr;
                    // Set forwardingPtr on predecessor
                    if (predecessor instanceof InternalNode) {
                        ((InternalNode<K,V>) predecessor).forwardingPtr = finalTarget;
                    } else if (predecessor instanceof LeafNode) {
                        ((LeafNode<K,V>) predecessor).forwardingPtr = finalTarget;
                    }
                    // Set reverse pointer on final target (points back to predecessor)
                    if (finalTarget.node instanceof InternalNode) {
                        ((InternalNode<K,V>) finalTarget.node).reversePtr = predecessor;
                    } else if (finalTarget.node instanceof LeafNode) {
                        ((LeafNode<K,V>) finalTarget.node).reversePtr = predecessor;
                    }
                    // Don't set info.p.forwardingPtr - it's skipped in chain
                } else {
                    // First link - create new chain
                    info.p.forwardingPtr = finalTarget;
                    // Set reverse pointer on final target (points back to info.p)
                    if (finalTarget.node instanceof InternalNode) {
                        ((InternalNode<K,V>) finalTarget.node).reversePtr = info.p;
                    } else if (finalTarget.node instanceof LeafNode) {
                        ((LeafNode<K,V>) finalTarget.node).reversePtr = info.p;
                    }
                }
                // Metadata updates happen in caller via fastUpdateMetadataOnly
            } else {
                // Slow path: Full propagation (updates both structure and nbChild)
                propagate(info.gp);
            }
        }
        infoUpdater.compareAndSet(info.gp, info, new Clean<>());
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

    /**
     * Update ONLY fastSize along the path from the given node to the root.
     * MyBSTnext does NOT update Version tree structure in fast path - uses forwarding pointers instead.
     */
    private void fastUpdateMetadataOnly(int delta, Node<K,V> startNode) {
        Node<K,V> current = startNode;
        while (current != null) {
            if (current instanceof InternalNode) {
                InternalNode<K,V> internal = (InternalNode<K,V>) current;
                // Update fastSize only - no Version tree updates
                internal.fastSize.addAndGet(delta);
            }
            // Leaf nodes don't need updates - their fastSize is fixed at creation
            current = current.parent;
        }
    }
    

    //--------------------------------------------------------------------------------
    // HANDSHAKE MECHANISM
    //--------------------------------------------------------------------------------
    
    private void setOpPhaseIdle() {
        int tid = threadID.get();
        if (tid < MAX_THREADS) {
            opPhase.get(tid).set(IDLE_PHASE);
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
            
            opPhase.get(tid).set(phase);
        }
    }
    
    private long getQueriesPhase() {
        return queriesPhase.get();
    }
    
    private void performHandshake(long targetPhase) {
        long startTime = System.nanoTime();
        
        // Only check threads that have been active (optimization)
        int activeThreads = (int)maxThreadID.get() + 1;
        if (activeThreads <= 0) return;  // No threads have started yet
        
        for (int tid = 0; tid < activeThreads; tid++) {
            long phase;
            while ((phase = opPhase.get(tid).get()) != IDLE_PHASE && phase < targetPhase) {
                // Spin wait for thread to acknowledge handshake
                Thread.onSpinWait();
            }
        }
        
        long elapsed = System.nanoTime() - startTime;
        totalHandshakes.incrementAndGet();
        totalHandshakeTimeNanos.addAndGet(elapsed);
    }
    
    private long enterSlowPath() {
        // Increment reader count first
        activeReaders.incrementAndGet();
        
        long currQueriesPhase = queriesPhase.get();
        
        // If already in slow path ((phase & 3) == 2), use CAS to skip to next slow path
        // This prevents a race where old thread wakes up and incorrectly transitions to fast
        if ((currQueriesPhase & 3) == 2) {
            // Try CAS to move to next slow path (phase 2 → phase 6, both slow path)
            queriesPhase.compareAndSet(currQueriesPhase, currQueriesPhase + 4);
            // Re-read phase (either we succeeded CAS, or another thread changed it)
            return queriesPhase.get();
        }
        
        // Case 2: Wait for slow path transition to complete
        while ((currQueriesPhase & 3) != 0) {
            currQueriesPhase = queriesPhase.get();
            // If transition completed to slow path, return current phase
            if ((currQueriesPhase & 3) == 2) {
                return currQueriesPhase;  // Just return - no CAS to next cycle
            }
        }
        
        // Try to be the thread that performs the transition
        // Use CAS to avoid multiple threads doing handshakes simultaneously
        if (queriesPhase.compareAndSet(currQueriesPhase, currQueriesPhase + 1)) {
            // We won the race, perform the handshakes
            // First handshake: switching phase (currQueriesPhase + 1)
            performHandshake(currQueriesPhase + 1);
            
            // Second handshake: move to slow path (currQueriesPhase + 2)
            queriesPhase.set(currQueriesPhase + 2);
            performHandshake(currQueriesPhase + 2);
            
            return currQueriesPhase + 2;
        } else {
            // Another thread is doing the transition, wait for slow path
            do {
                currQueriesPhase = queriesPhase.get();
            } while ((currQueriesPhase & 3) != 2);
            return currQueriesPhase;
        }
    }
    
    /**
     * Exit slow path protocol.
     * Decrements activeReaders counter and if this is the last reader,
     * transitions back to fast path (phase 2 → phase 4 ≡ 0 mod 4).
     * 
     * @param currPhase The phase that was captured when entering slow path
     */
    private void exitSlowPath(long currPhase) {
        long remainingReaders = activeReaders.decrementAndGet();
        
        // If we're the last reader to finish, try to transition back to fast path
        if (remainingReaders == 0 && (currPhase & 3) == 2) {
            // Try CAS to return to fast path (increment by 2: phase 2 → 4 → 0 mod 4)
            // Use currPhase (the phase we entered with) for the CAS
            queriesPhase.compareAndSet(currPhase, currPhase + 2);
        }
    }
    
    
    
    //--------------------------------------------------------------------------------
    // FAST AND SLOW PATH OPERATIONS
    //--------------------------------------------------------------------------------
    
    
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

    /**
     * Helper to compute size of a Version subtree.
     * Combines slow path metadata (nbChild) with fast path metadata (fastSize).
     */
    /**
     * Helper to compute size of a Version subtree.
     * Combines slow path metadata (nbChild) with fast path metadata (fastSize).
     * Matches pseudocode line 432-435.
     * 
     * IMPORTANT: If the Version's node has a forwardingPtr, follow it to get the complete subtree size.
     */
    private int computeSubtreeSize(Version<K> version) {
        if (version == null) return 0;
        
        // Check if this version's node has been orphaned (has forwardingPtr)
        Version<K> targetVersion = version;
        if (version.node instanceof InternalNode) {
            InternalNode<K,V> internal = (InternalNode<K,V>) version.node;
            if (internal.forwardingPtr != null) {
                targetVersion = internal.forwardingPtr;
            }
        } else if (version.node instanceof LeafNode) {
            LeafNode<K,V> leaf = (LeafNode<K,V>) version.node;
            if (leaf.forwardingPtr != null) {
                targetVersion = leaf.forwardingPtr;
            }
        }
        
        // Combine slow (nbChild) and fast (fastSize) metadata from target version
        long fastSize = 0;
        if (targetVersion.node instanceof InternalNode) {
            fastSize = ((InternalNode<K,V>) targetVersion.node).fastSize.get();
        } else if (targetVersion.node instanceof LeafNode) {
            fastSize = ((LeafNode<K,V>) targetVersion.node).fastSize.get();
        }
        return targetVersion.nbChild + (int)fastSize;
    }
    
    public int sizeSnapshot() {
        totalSizeCalls.incrementAndGet();
        
        // Enter slow path (performs handshakes if needed, increments reader count)
        // Capture the phase we entered with for proper exit
        long currPhase = enterSlowPath();
        
        try {
            // Use helper to compute size
            return computeSubtreeSize(root.version);
        } finally {
            // Exit slow path using the captured phase
            exitSlowPath(currPhase);
        }
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

    /**
     * Rank query using fast path metadata (fastSize in nodes).
     * Returns the rank (1-based position) of the given key in sorted order.
     * Returns -1 if key is not found.
     * 
     * Navigation uses immutable Version tree for consistency.
     * Linearization point: Reading root.version after enterSlowPath().
     */
    public int rank(K key) {
        if (key == null) return -1;
        
        // Enter slow path to ensure consistency
        // Capture the phase we entered with for proper exit
        long currPhase = enterSlowPath();
        
        try {
            // LINEARIZATION POINT: Read root.version to get immutable snapshot
            Version<K> snapshot = root.version;
            
            if (snapshot == null) return -1;
            
            Version<K> current = snapshot;
            int rank = 0;
            
            // Navigate Version tree until we reach a leaf (current.left == null)
            while (true) {
                // ALWAYS check for forwarding pointer first (even on leaves!)
                Version<K> forwardPtr = null;
                if (current.node instanceof InternalNode) {
                    forwardPtr = ((InternalNode<K,V>) current.node).forwardingPtr;
                } else if (current.node instanceof LeafNode) {
                    forwardPtr = ((LeafNode<K,V>) current.node).forwardingPtr;
                }
                
                if (forwardPtr != null) {
                    // This node is orphaned - jump to replacement
                    current = forwardPtr;
                    // Loop again to check new current
                    continue;
                }
                
                // No forwarding pointer - check if we've reached a leaf
                if (current.left == null) {
                    // Reached a leaf, stop navigation
                    break;
                }
                
                // Internal node - navigate left or right based on key comparison
                if (current.key == null || key.compareTo(current.key) < 0) {
                    // Go left in Version tree
                    current = current.left;
                } else {
                    // Go right - add left subtree size to rank using helper
                    int leftSubtreeSize = computeSubtreeSize(current.left);
                    rank += leftSubtreeSize;
                    current = current.right;
                }
                // Loop continues - will check forwarding pointer on new current
            }
            
            // We've reached a leaf in the Version tree - check if it's our key
            if (current != null && current.key != null && key.compareTo(current.key) == 0) {
                return rank + 1; // 1-based rank
            }
            
            return -1; // Key not found
        } finally {
            // Exit slow path using the captured phase
            exitSlowPath(currPhase);
        }
    }

    /**
     * Select query using Version tree navigation.
     * Returns the kth smallest key (1-based indexing).
     * Returns null if k is out of range.
     * 
     * Navigation uses immutable Version tree for consistency.
     * Linearization point: Reading root.version after enterSlowPath().
     */
    public K select(int k) {
        if (k <= 0) return null;
        
        // Enter slow path to ensure consistency
        // Capture the phase we entered with for proper exit
        long currPhase = enterSlowPath();
        
        try {
            // LINEARIZATION POINT: Read root.version to get immutable snapshot
            Version<K> snapshot = root.version;
            
            // If no snapshot exists yet (no updates in slow path), build it now
            if (snapshot == null) {
                System.out.println("Empty snapshot in select()");
            }
            
            Version<K> current = snapshot;
            int remaining = k;
            
            // Navigate Version tree until we reach a leaf (current.left == null)
            while (true) {
                // ALWAYS check for forwarding pointer first (even on leaves!)
                Version<K> forwardPtr = null;
                if (current.node instanceof InternalNode) {
                    forwardPtr = ((InternalNode<K,V>) current.node).forwardingPtr;
                } else if (current.node instanceof LeafNode) {
                    forwardPtr = ((LeafNode<K,V>) current.node).forwardingPtr;
                }
                
                if (forwardPtr != null) {
                    // This node is orphaned - jump to replacement
                    current = forwardPtr;
                    // Loop again to check new current
                    continue;
                }
                
                // No forwarding pointer - check if we've reached a leaf
                if (current.left == null) {
                    // Reached a leaf, stop navigation
                    break;
                }
                
                // Internal node - navigate left or right based on remaining count
                // Calculate left subtree size using helper
                int leftSize = computeSubtreeSize(current.left);
                
                if (remaining <= leftSize) {
                    // Target is in left subtree - navigate via Version tree
                    current = current.left;
                } else {
                    // Target is in right subtree - navigate via Version tree
                    remaining -= leftSize;
                    current = current.right;
                }
                // Loop continues - will check forwarding pointer on new current
            }
            
            // We've reached a leaf in the Version tree - check if it's the kth element
            if (current != null && current.key != null && remaining == 1) {
                return current.key;
            }
            
            return null; // Out of range
        } finally {
            // Exit slow path using the captured phase
            exitSlowPath(currPhase);
        }
    }
}
