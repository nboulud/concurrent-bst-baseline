package bench;

import bst.MyBST;
import java.util.Random;
import java.util.concurrent.*;

public class MicroBenchHandshake {

    // Interface for key-value operations
    interface KV {
        void insert(int k);
        void delete(int k);
        Integer get(int k);
    }

    static class HandshakeBSTKV implements KV {
        private final MyBST<Integer,Integer> map = new MyBST<>();
        public void insert(int k) { map.put(k, k); }
        public void delete(int k) { map.remove(k); }
        public Integer get(int k) { return map.get(k); }
    }

    public static void main(String[] args) throws Exception {
        int threads = (args.length >= 1) ? Integer.parseInt(args[0]) : Runtime.getRuntime().availableProcessors();
        int seconds = (args.length >= 2) ? Integer.parseInt(args[1]) : 3;

        // Using MyBST with handshake mechanism
        KV ds = new HandshakeBSTKV();

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

        System.out.printf("Handshake MyBST: Threads=%d, Time=%ds, TotalOps=%d, Throughput=%.2f Mops/s%n",
                threads, seconds, totalOps, mopsPerSec);
    }
}

