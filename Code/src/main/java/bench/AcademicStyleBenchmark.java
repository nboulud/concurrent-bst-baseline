package bench;

import bst.MyBSTnext;
import bst.MyBSTBaseline;
import java.io.FileOutputStream;
import java.io.PrintStream;
import java.lang.management.GarbageCollectorMXBean;
import java.lang.management.ManagementFactory;
import java.lang.management.ThreadMXBean;
import java.util.List;
import java.util.Random;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Academic-style benchmark following ConcurrentSizeMethods framework.
 * Compares MyBST vs MyBSTBaseline with comprehensive metrics.
 * Tests insert, delete, contains, size, rank, and select operations.
 */
public class AcademicStyleBenchmark {
    
    static class WorkloadConfig {
        final int insertThreads;
        final int deleteThreads;
        final int containsThreads;
        final int sizeThreads;
        final int rankThreads;
        final int selectThreads;
        
        WorkloadConfig(int ins, int del, int con, int size, int rank, int select) {
            this.insertThreads = ins;
            this.deleteThreads = del;
            this.containsThreads = con;
            this.sizeThreads = size;
            this.rankThreads = rank;
            this.selectThreads = select;
        }
        
        int totalThreads() {
            return insertThreads + deleteThreads + containsThreads + sizeThreads + rankThreads + selectThreads;
        }
        
        @Override
        public String toString() {
            return String.format("ins%d-del%d-con%d-size%d-rank%d-sel%d", 
                insertThreads, deleteThreads, containsThreads, sizeThreads, rankThreads, selectThreads);
        }
    }
    
    enum OpType {
        INSERT, DELETE, CONTAINS, SIZE, RANK, SELECT
    }
    
    interface BSTAdapter {
        boolean insert(int key);
        boolean delete(int key);
        boolean contains(int key);
        int size();
        int rank(int key);
        Integer select(int k);
    }
    
    static class MyBSTWrapper implements BSTAdapter {
        private final MyBSTnext<Integer, Integer> tree = new MyBSTnext<>();
        
        @Override
        public boolean insert(int key) {
            return tree.put(key, key) == null;
        }
        
        @Override
        public boolean delete(int key) {
            return tree.remove(key) != null;
        }
        
        @Override
        public boolean contains(int key) {
            return tree.get(key) != null;
        }
        
        @Override
        public int size() {
            return tree.sizeSnapshot();
        }
        
        @Override
        public int rank(int key) {
            return tree.rank(key);
        }
        
        @Override
        public Integer select(int k) {
            return tree.select(k);
        }
    }
    
    static class MyBSTBaselineWrapper implements BSTAdapter {
        private final MyBSTBaseline<Integer, Integer> tree = new MyBSTBaseline<>();
        
        @Override
        public boolean insert(int key) {
            return tree.put(key, key) == null;
        }
        
        @Override
        public boolean delete(int key) {
            return tree.remove(key) != null;
        }
        
        @Override
        public boolean contains(int key) {
            return tree.get(key) != null;
        }
        
        @Override
        public int size() {
            return tree.sizeSnapshot();
        }
        
        @Override
        public int rank(int key) {
            return tree.rank(key);
        }
        
        @Override
        public Integer select(int k) {
            return tree.select(k);
        }
    }
    
    static class Worker extends Thread {
        private final int id;
        private final BSTAdapter tree;
        private final CyclicBarrier barrier;
        private final AtomicBoolean running;
        private final double secondsToRun;
        private final OpType opType;
        private final int maxKey;
        private final Random rng;
        private final ThreadMXBean threadMXBean;
        
        // Timing
        private long myStartUserTime, myStartWallTime, myStartCPUTime;
        private long userTime, wallTime, cpuTime;
        private long insTime, delTime, containsTime, sizeTime, rankTime, selectTime;
        
        // Counters
        private long trueIns, falseIns;
        private long trueDel, falseDel;
        private long trueContains, falseContains;
        private long doneSize, doneRank, doneSelect;
        
        Worker(int id, BSTAdapter tree, CyclicBarrier barrier, AtomicBoolean running,
               double secondsToRun, OpType opType, int maxKey) {
            this.id = id;
            this.tree = tree;
            this.barrier = barrier;
            this.running = running;
            this.secondsToRun = secondsToRun;
            this.opType = opType;
            this.maxKey = maxKey;
            this.rng = new Random(id * 31L);
            this.threadMXBean = ManagementFactory.getThreadMXBean();
        }
        
        @Override
        public void run() {
            try {
                barrier.await(); // Wait for all threads to be ready
                
                // Record start times
                myStartUserTime = threadMXBean.getCurrentThreadUserTime();
                myStartWallTime = System.nanoTime();
                myStartCPUTime = threadMXBean.getCurrentThreadCpuTime();
                
                long deadline = System.nanoTime() + (long)(secondsToRun * 1e9);
                
                // Each thread does ONLY its assigned operation type
                while (running.get() && System.nanoTime() < deadline) {
                    switch (opType) {
                        case INSERT:
                            executeInsertOp();
                            break;
                        case DELETE:
                            executeDeleteOp();
                            break;
                        case CONTAINS:
                            executeContainsOp();
                            break;
                        case SIZE:
                            executeSizeOp();
                            // Small pause for query operations
                            try { Thread.sleep(0, 1000); } catch (InterruptedException e) {}
                            break;
                        case RANK:
                            executeRankOp();
                            // Small pause for query operations
                            try { Thread.sleep(0, 1000); } catch (InterruptedException e) {}
                            break;
                        case SELECT:
                            executeSelectOp();
                            // Small pause for query operations
                            try { Thread.sleep(0, 1000); } catch (InterruptedException e) {}
                            break;
                    }
                }
                
                // Record end times
                userTime = threadMXBean.getCurrentThreadUserTime();
                wallTime = System.nanoTime();
                cpuTime = threadMXBean.getCurrentThreadCpuTime();
                
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        
        private void executeInsertOp() {
            int key = rng.nextInt(maxKey);
            long start = System.nanoTime();
            boolean result = tree.insert(key);
            insTime += System.nanoTime() - start;
            if (result) trueIns++;
            else falseIns++;
        }
        
        private void executeDeleteOp() {
            int key = rng.nextInt(maxKey);
            long start = System.nanoTime();
            boolean result = tree.delete(key);
            delTime += System.nanoTime() - start;
            if (result) trueDel++;
            else falseDel++;
        }
        
        private void executeContainsOp() {
            int key = rng.nextInt(maxKey);
            long start = System.nanoTime();
            boolean result = tree.contains(key);
            containsTime += System.nanoTime() - start;
            if (result) trueContains++;
            else falseContains++;
        }
        
        private void executeSizeOp() {
            long start = System.nanoTime();
            tree.size();
            sizeTime += System.nanoTime() - start;
            doneSize++;
        }
        
        private void executeRankOp() {
            long start = System.nanoTime();
            int key = rng.nextInt(maxKey);
            tree.rank(key);
            rankTime += System.nanoTime() - start;
            doneRank++;
        }
        
        private void executeSelectOp() {
            long start = System.nanoTime();
            int k = rng.nextInt(10000) + 1; // Reasonable range for select
            tree.select(k);
            selectTime += System.nanoTime() - start;
            doneSelect++;
        }
        
        // Getters
        public long getMyStartUserTime() { return myStartUserTime; }
        public long getMyStartWallTime() { return myStartWallTime; }
        public long getMyStartCPUTime() { return myStartCPUTime; }
        public long getUserTime() { return userTime; }
        public long getWallTime() { return wallTime; }
        public long getCPUTime() { return cpuTime; }
        public long getInsTime() { return insTime; }
        public long getDelTime() { return delTime; }
        public long getContainsTime() { return containsTime; }
        public long getSizeTime() { return sizeTime; }
        public long getRankTime() { return rankTime; }
        public long getSelectTime() { return selectTime; }
        public long getTrueIns() { return trueIns; }
        public long getFalseIns() { return falseIns; }
        public long getTrueDel() { return trueDel; }
        public long getFalseDel() { return falseDel; }
        public long getTrueContains() { return trueContains; }
        public long getFalseContains() { return falseContains; }
        public long getDoneSize() { return doneSize; }
        public long getDoneRank() { return doneRank; }
        public long getDoneSelect() { return doneSelect; }
    }
    
    static void prefillTree(BSTAdapter tree, int targetSize, int maxKey) {
        Random rng = new Random(42);
        int inserted = 0;
        while (inserted < targetSize) {
            int key = rng.nextInt(maxKey);
            if (tree.insert(key)) {
                inserted++;
            }
        }
    }
    
    static long totalGCTime() {
        List<GarbageCollectorMXBean> gcBeans = ManagementFactory.getGarbageCollectorMXBeans();
        long total = 0;
        for (GarbageCollectorMXBean bean : gcBeans) {
            total += bean.getCollectionTime();
        }
        return total;
    }
    
    static void runTrial(PrintStream out, String algName, BSTAdapter tree, 
                        WorkloadConfig config, double seconds,
                        int initSize, int maxKey, int trialNum) throws Exception {
        
        // Prefill
        prefillTree(tree, initSize, maxKey);
        
        int totalThreads = config.totalThreads();
        Worker[] workers = new Worker[totalThreads];
        CyclicBarrier barrier = new CyclicBarrier(totalThreads + 1);
        AtomicBoolean running = new AtomicBoolean(true);
        
        int idx = 0;
        
        // Create INSERT threads
        for (int i = 0; i < config.insertThreads; i++) {
            workers[idx++] = new Worker(idx, tree, barrier, running, seconds, OpType.INSERT, maxKey);
        }
        
        // Create DELETE threads
        for (int i = 0; i < config.deleteThreads; i++) {
            workers[idx++] = new Worker(idx, tree, barrier, running, seconds, OpType.DELETE, maxKey);
        }
        
        // Create CONTAINS threads
        for (int i = 0; i < config.containsThreads; i++) {
            workers[idx++] = new Worker(idx, tree, barrier, running, seconds, OpType.CONTAINS, maxKey);
        }
        
        // Create SIZE threads
        for (int i = 0; i < config.sizeThreads; i++) {
            workers[idx++] = new Worker(idx, tree, barrier, running, seconds, OpType.SIZE, maxKey);
        }
        
        // Create RANK threads
        for (int i = 0; i < config.rankThreads; i++) {
            workers[idx++] = new Worker(idx, tree, barrier, running, seconds, OpType.RANK, maxKey);
        }
        
        // Create SELECT threads
        for (int i = 0; i < config.selectThreads; i++) {
            workers[idx++] = new Worker(idx, tree, barrier, running, seconds, OpType.SELECT, maxKey);
        }
        
        // Start all threads
        for (Worker w : workers) {
            w.start();
        }
        
        long gcStart = totalGCTime();
        
        // Release all threads
        barrier.await();
        
        // Wait for completion
        Thread.sleep((long)(seconds * 1000));
        running.set(false);
        
        for (Worker w : workers) {
            w.join();
        }
        
        long gcEnd = totalGCTime();
        
        // Compute metrics
        long totalOps = 0;
        
        for (Worker w : workers) {
            long ops = w.getTrueIns() + w.getFalseIns() + w.getTrueDel() + w.getFalseDel() + 
                      w.getTrueContains() + w.getFalseContains() + w.getDoneSize() + 
                      w.getDoneRank() + w.getDoneSelect();
            totalOps += ops;
        }
        
        long totalElapsedUser = 0, totalElapsedWall = 0, totalElapsedCPU = 0;
        for (Worker w : workers) {
            totalElapsedUser += w.getUserTime() - w.getMyStartUserTime();
            totalElapsedWall += w.getWallTime() - w.getMyStartWallTime();
            totalElapsedCPU += w.getCPUTime() - w.getMyStartCPUTime();
        }
        
        double gcTime = (gcEnd - gcStart) / 1e3;
        
        // Output CSV line
        out.print(algName);
        out.print("," + config.toString());
        out.print("," + totalThreads);
        out.print("," + (totalOps / seconds));
        out.print("," + (totalElapsedUser / 1e9));
        out.print("," + (totalElapsedWall / 1e9));
        out.print("," + (totalElapsedCPU / 1e9));
        out.print("," + gcTime);
        out.print("," + seconds);
        
        // Per-operation throughput
        long totalIns = 0, totalDel = 0, totalCon = 0, totalSize = 0, totalRank = 0, totalSelect = 0;
        for (Worker w : workers) {
            totalIns += w.getTrueIns() + w.getFalseIns();
            totalDel += w.getTrueDel() + w.getFalseDel();
            totalCon += w.getTrueContains() + w.getFalseContains();
            totalSize += w.getDoneSize();
            totalRank += w.getDoneRank();
            totalSelect += w.getDoneSelect();
        }
        
        out.print("," + (totalIns / seconds));
        out.print("," + (totalDel / seconds));
        out.print("," + (totalCon / seconds));
        out.print("," + (totalSize / seconds));
        out.print("," + (totalRank / seconds));
        out.print("," + (totalSelect / seconds));
        
        out.println();
    }
    
    public static void main(String[] args) throws Exception {
        // Workload configurations - following OperationBenchmark style
        WorkloadConfig[] workloads = {
            // Similar to OperationBenchmark Workload 1: 100 threads with balanced mix
            new WorkloadConfig(10, 10, 50, 10, 10, 10),  // ins10-del10-con50-size10-rank10-sel10
            
            // Similar to OperationBenchmark Workload 2: less queries
            new WorkloadConfig(10, 10, 60, 6, 7, 7),     // ins10-del10-con60-size6-rank7-sel7
            
            // Similar to OperationBenchmark Workload 3: even less queries
            new WorkloadConfig(10, 10, 70, 3, 3, 4),     // ins10-del10-con70-size3-rank3-sel4
            
            // Similar to OperationBenchmark Workload 4: many queries
            new WorkloadConfig(10, 10, 26, 16, 17, 17),  // ins10-del10-con26-size16-rank17-sel17
            
            // No queries workloads for comparison
            new WorkloadConfig(30, 20, 50, 0, 0, 0),     // ins30-del20-con50 (no queries)
            new WorkloadConfig(3, 2, 95, 0, 0, 0),       // ins3-del2-con95 (no queries)
        };
        
        int nTrials = 3;
        double seconds = 5.0;
        int initSize = 10000;
        int maxKey = 200000;
        
        String filename = "academic_benchmark_results.csv";
        PrintStream out = new PrintStream(new FileOutputStream(filename));
        
        // CSV header
        out.println("name,workload,totalThreads,totalThroughput," +
                   "totalUserTime,totalWallTime,totalCPUTime,gcTime,seconds," +
                   "insThroughput,delThroughput,containsThroughput,sizeThroughput,rankThroughput,selectThroughput");
        
        System.out.println("╔══════════════════════════════════════════════════════════════════════════╗");
        System.out.println("║          ACADEMIC-STYLE BENCHMARK (MyBST vs MyBSTBaseline)              ║");
        System.out.println("║           Dedicated threads per operation (like OperationBench)         ║");
        System.out.println("║              Queries have small pauses between iterations               ║");
        System.out.println("╚══════════════════════════════════════════════════════════════════════════╝");
        System.out.println();
        
        int totalTests = workloads.length * nTrials * 2;
        int completed = 0;
        
        for (WorkloadConfig config : workloads) {
            for (int trial = 0; trial < nTrials; trial++) {
                // Test MyBSTBaseline
                System.out.printf("[%d/%d] MyBSTBaseline %s trial=%d... ",
                    ++completed, totalTests, config, trial+1);
                System.out.flush();
                
                MyBSTBaselineWrapper baseline = new MyBSTBaselineWrapper();
                runTrial(out, "MyBSTBaseline", baseline, config,
                        seconds, initSize, maxKey, trial);
                
                System.out.println("✓");
                
                // Test MyBST
                System.out.printf("[%d/%d] MyBST %s trial=%d... ",
                    ++completed, totalTests, config, trial+1);
                System.out.flush();
                
                MyBSTWrapper mybst = new MyBSTWrapper();
                runTrial(out, "MyBST", mybst, config,
                        seconds, initSize, maxKey, trial);
                
                System.out.println("✓");
            }
        }
        
        out.close();
        System.out.println();
        System.out.println("✅ Benchmark complete! Results saved to: " + filename);
        System.out.println();
        System.out.println("To analyze results:");
        System.out.println("  python3 analyze_academic_benchmark.py");
    }
}
