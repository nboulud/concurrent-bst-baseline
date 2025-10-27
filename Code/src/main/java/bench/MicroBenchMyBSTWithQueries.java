package bench;

import bst.MyBSTBaseline;
import java.util.Random;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;

public class MicroBenchMyBSTWithQueries {

    static class MyBSTBaselineKV {
        private final MyBSTBaseline<Integer,Integer> map = new MyBSTBaseline<>();
        public void insert(int k) { map.put(k, k); }
        public void delete(int k) { map.remove(k); }
        public Integer get(int k) { return map.get(k); }
        public int size() { return map.sizeSnapshot(); }
    }

    public static void main(String[] args) throws Exception {
        int threads = (args.length >= 1) ? Integer.parseInt(args[0]) : Runtime.getRuntime().availableProcessors();
        int seconds = (args.length >= 2) ? Integer.parseInt(args[1]) : 3;

        // Using MyBSTBaseline with eager propagation
        MyBSTBaselineKV ds = new MyBSTBaselineKV();

        // Preload
        System.out.println("Preloading 50k elements...");
        for (int i = 0; i < 50_000; i++) ds.insert(i);
        
        // Verify initial size
        int initialSize = ds.size();
        System.out.println("Initial size: " + initialSize);

        ExecutorService pool = Executors.newFixedThreadPool(threads);
        CountDownLatch start = new CountDownLatch(1);
        CountDownLatch stop = new CountDownLatch(threads);

        final long endAt = System.nanoTime() + TimeUnit.SECONDS.toNanos(seconds);
        final ConcurrentLinkedQueue<Long> counts = new ConcurrentLinkedQueue<>();
        final ConcurrentLinkedQueue<Long> sizeQueries = new ConcurrentLinkedQueue<>();
        final AtomicLong sizeErrorCount = new AtomicLong(0);

        for (int t = 0; t < threads; t++) {
            pool.submit(() -> {
                try {
                    Random rnd = new Random(Thread.currentThread().getId());
                    long ops = 0;
                    long sizeOps = 0;
                    start.await();
                    while (System.nanoTime() < endAt) {
                        int k = rnd.nextInt(200_000);
                        int r = rnd.nextInt(100);
                        // 75% gets, 10% inserts, 10% deletes, 5% size queries
                        if (r < 75) { 
                            ds.get(k); 
                        }
                        else if (r < 85) { 
                            ds.insert(k); 
                        }
                        else if (r < 95) { 
                            ds.delete(k); 
                        }
                        else { 
                            // Size query
                            int size = ds.size();
                            if (size < 0 || size > 200_000) {
                                sizeErrorCount.incrementAndGet();
                            }
                            sizeOps++;
                        }
                        ops++;
                    }
                    counts.add(ops);
                    sizeQueries.add(sizeOps);
                } catch (InterruptedException ignored) { }
                finally { stop.countDown(); }
            });
        }

        start.countDown();
        stop.await();
        pool.shutdown();

        long totalOps = counts.stream().mapToLong(Long::longValue).sum();
        long totalSizeQueries = sizeQueries.stream().mapToLong(Long::longValue).sum();
        double mopsPerSec = totalOps / (double)seconds / 1_000_000.0;
        
        // Final verification
        int finalSize = ds.size();

        System.out.printf("MyBSTBaseline (eager): Threads=%d, Time=%ds, TotalOps=%d, SizeQueries=%d, Throughput=%.2f Mops/s%n",
                threads, seconds, totalOps, totalSizeQueries, mopsPerSec);
        System.out.println("Final size: " + finalSize);
        System.out.println("Size errors detected: " + sizeErrorCount.get());
        System.out.println("✓ Size operation completed!");
    }
}

