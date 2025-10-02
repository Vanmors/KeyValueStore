package dev.kvstore.core;

import dev.kvstore.core.LSM.LSMEngine;
import dev.kvstore.core.LSM.LSMEngineImpl;
import dev.kvstore.core.model.PutOptions;
import dev.kvstore.core.model.ReadOptions;
import org.junit.jupiter.api.*;

import java.io.IOException;
import java.nio.file.Files;
import java.util.concurrent.*;
import java.util.stream.IntStream;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class LSMEngineConcurrencyTest {

    private java.nio.file.Path tmp;
    private LSMEngine engine;

    @BeforeEach
    void setUp() throws IOException {
        tmp = Files.createTempDirectory("lsm-concurrency-");
        // чтобы чаще происходил flush
        engine = new LSMEngineImpl(tmp.toString(), 4_096);
    }

    @AfterEach
    void tearDown() {
        // close() нет — ничего не делаем
    }

    @Test
    void parallelPutsAndGets() throws Exception {
        final int threads = 6;
        final int keysPerThread = 400;

        ExecutorService pool = Executors.newFixedThreadPool(threads);
        CountDownLatch go = new CountDownLatch(1);

        for (int t = 0; t < threads; t++) {
            final int tid = t;
            pool.submit(() -> {
                try {
                    go.await();
                    for (int k = 0; k < keysPerThread; k++) {
                        String key = "t" + tid + "_k" + k;
                        String val = "v" + tid + "_" + k;
                        engine.put(key.getBytes(), val.getBytes(), PutOptions.DEFAULT);

                        if ((k % 50) == 0) {
                            final var got = engine.get(key.getBytes(), ReadOptions.DEFAULT);
                            assertEquals(val, new String(got.value()));
                        }
                    }
                } catch (InterruptedException | KVException | IOException e) {
                    throw new RuntimeException(e);
                }
            });
        }

        go.countDown();
        pool.shutdown();
        pool.awaitTermination(30, TimeUnit.SECONDS);

        // выборочно проверим последние ключи
        for (int t = 0; t < threads; t++) {
            for (int k = 0; k < 5; k++) {
                String key = "t" + t + "_k" + (keysPerThread - 1 - k);
                String val = "v" + t + "_" + (keysPerThread - 1 - k);
                final var got = engine.get(key.getBytes(), ReadOptions.DEFAULT);
                assertEquals(val, new String(got.value()));
            }
        }
    }

    @Test
    void readsDuringFlushAndCompact() throws Exception {
        // заполним memtable
        IntStream.range(0, 1_000).forEach(i -> {
            try {
                engine.put(("k" + i).getBytes(), ("v" + i).getBytes(), PutOptions.DEFAULT);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        });

        ExecutorService pool = Executors.newFixedThreadPool(2);
        Future<?> f1 = pool.submit(() -> {
            try {
                engine.flush();
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        });
        Future<?> f2 = pool.submit(() -> {
            try {
                for (int i = 0; i < 1_000; i++) {
                    final var got = engine.get(("k" + i).getBytes(), ReadOptions.DEFAULT);
                    assertEquals("v" + i, new String(got.value()));
                }
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        });

        f1.get(10, TimeUnit.SECONDS);
        f2.get(10, TimeUnit.SECONDS);

        // создадим много SSTable, чтобы триггернуть compact(0)
        for (int r = 0; r < 4; r++) {
            for (int i = 0; i < 500; i++) {
                engine.put(("c" + r + ":" + i).getBytes(), ("val" + r + "_" + i).getBytes(), PutOptions.DEFAULT);
            }
            engine.flush(); // flush сам вызовет compact(0), если файлов > 3
        }

        // контрольное чтение после возможной компакции
        for (int i = 0; i < 1_000; i++) {
            final var got = engine.get(("k" + i).getBytes(), ReadOptions.DEFAULT);
            assertEquals("v" + i, new String(got.value()));
        }

        pool.shutdownNow();
    }
}
