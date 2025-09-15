package bench;

import bst.LockFreeBSTMap;  // or import bst.BST if you want to benchmark the baseline instead
import java.util.Random;
import bst.BST;
import java.util.concurrent.*;

public class MicroBench {

    // Change these to test the baseline set instead:
    interface KV {
        void insert(int k);
        void delete(int k);
        Integer get(int k);
    }

    static class LockFreeKV implements KV {
        private final LockFreeBSTMap<Integer,Integer> map = new LockFreeBSTMap<>();
        public void insert(int k) { map.put(k, k); }
        public void delete(int k) { map.remove(k); }
        public Integer get(int k) { return map.get(k); }
    }

    static class BaselineKV implements KV {
        private final BST<Integer,Integer> map = new BST<>();
        public void insert(int k) { map.put(k, k); }
        public void delete(int k) { map.remove(k); }
        public Integer get(int k) { return map.get(k); }
    }


    public static void main(String[] args) throws Exception {
        int threads = (args.length >= 1) ? Integer.parseInt(args[0]) : Runtime.getRuntime().availableProcessors();
        int seconds = (args.length >= 2) ? Integer.parseInt(args[1]) : 3;

        // Choose which structure to test:
        KV ds = new BaselineKV();

        // Preload a bit
        for (int i = 0; i < 50_000; i++) ds.insert(i);

        ExecutorService pool = Executors.newFixedThreadPool(threads);
        CountDownLatch start = new CountDownLatch(1);
        CountDownLatch stop = new CountDownLatch(threads);

        final long endAt = System.nanoTime() + TimeUnit.SECONDS.toNanos(seconds);
        final ConcurrentLinkedQueue<Long> counts = new ConcurrentLinkedQueue<>();

        for (int t = 0; t < threads; t++) {
            pool.submit(() -> {
                try {
                    Random rnd = new Random(Thread.currentThread().getId());
                    long ops = 0;
                    start.await();
                    while (System.nanoTime() < endAt) {
                        int k = rnd.nextInt(200_000);
                        int r = rnd.nextInt(100);
                        // 80% gets, 10% inserts, 10% deletes
                        if (r < 80) { ds.get(k); }
                        else if (r < 90) { ds.insert(k); }
                        else { ds.delete(k); }
                        ops++;
                    }
                    counts.add(ops);
                } catch (InterruptedException ignored) { }
                finally { stop.countDown(); }
            });
        }

        start.countDown();
        stop.await();
        pool.shutdown();

        long totalOps = counts.stream().mapToLong(Long::longValue).sum();
        double mopsPerSec = totalOps / (double)seconds / 1_000_000.0;

        System.out.printf("Threads=%d, Time=%ds, TotalOps=%d, Throughput=%.2f Mops/s%n",
                threads, seconds, totalOps, mopsPerSec);
    }
}
