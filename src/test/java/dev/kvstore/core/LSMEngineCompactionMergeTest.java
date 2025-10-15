package dev.kvstore.core;

import dev.kvstore.core.LSM.LSMEngine;
import dev.kvstore.core.LSM.LSMEngineImpl;
import dev.kvstore.core.model.PutOptions;
import dev.kvstore.core.model.ReadOptions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.util.concurrent.TimeUnit;
import java.util.stream.IntStream;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class LSMEngineCompactionMergeTest {

    private java.nio.file.Path tmp;
    private LSMEngine engine;

    @BeforeEach
    void setUp() throws IOException {
        tmp = Files.createTempDirectory("lsm-merge-");
        engine = new LSMEngineImpl(tmp.toString(), 2_048);
    }

    @Test
    void overlappingKeysAreMergedNewestWins() throws Exception {
        // первая партия
        IntStream.range(0, 200).forEach(i -> {
            try {
                engine.put(("user:" + i).getBytes(), ("v1_" + i).getBytes(), PutOptions.DEFAULT);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        });
        engine.flush(); // SST #1

        // вторая партия с перекрытием
        IntStream.range(100, 300).forEach(i -> {
            try {
                engine.put(("user:" + i).getBytes(), ("v2_" + i).getBytes(), PutOptions.DEFAULT);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        });
        engine.flush(); // SST #2

        // до компакции get уже должен видеть самое новое
        assertEquals("v2_150", new String(engine.get("user:150".getBytes(), ReadOptions.DEFAULT).value()));

        // создадим ещё SSTable, чтобы гарантировать наличие 3 файлов и триггернуть compact(0)
        for (int r = 0; r < 2; r++) {
            for (int i = 300; i < 350; i++) {
                engine.put(("user:" + i).getBytes(), ("v2_" + i).getBytes(), PutOptions.DEFAULT);
            }
            engine.flush();
        }

        // чуть подождём компакцию
        TimeUnit.SECONDS.sleep(1);

        // проверяем что новее
        assertEquals("v1_50", new String(engine.get("user:50".getBytes(), ReadOptions.DEFAULT).value()));   // только в первой партии
        assertEquals("v2_150", new String(engine.get("user:150".getBytes(), ReadOptions.DEFAULT).value())); // перекрытие → v2
        assertEquals("v2_250", new String(engine.get("user:250".getBytes(), ReadOptions.DEFAULT).value())); // только во второй
    }
}
