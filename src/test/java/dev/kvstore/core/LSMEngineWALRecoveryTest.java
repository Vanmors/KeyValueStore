package dev.kvstore.core;

import dev.kvstore.core.LSM.LSMEngine;
import dev.kvstore.core.LSM.LSMEngineImpl;
import dev.kvstore.core.model.DeleteOptions;
import dev.kvstore.core.model.PutOptions;
import dev.kvstore.core.model.ReadOptions;
import org.junit.jupiter.api.Test;

import java.nio.file.Files;

import static org.junit.jupiter.api.Assertions.*;

public class LSMEngineWALRecoveryTest {

    @Test
    void recoverLatestStateFromWAL() throws Exception {
        var tmp = Files.createTempDirectory("lsm-wal-");

        // первая жизнь процесса
        LSMEngine e1 = new LSMEngineImpl(tmp.toString(), 8_192);
        e1.put("x".getBytes(), "1".getBytes(), PutOptions.DEFAULT);
        e1.put("y".getBytes(), "2".getBytes(), PutOptions.DEFAULT);
        e1.put("x".getBytes(), "3".getBytes(), PutOptions.DEFAULT); // обновили
        e1.delete("y".getBytes(), DeleteOptions.DEFAULT);           // удалили
        // имитируем внезапный краш

        // новый инстанс в той же директории
        LSMEngine e2 = new LSMEngineImpl(tmp.toString(), 8_192);

        // x должен быть 3, y отсутствовать
        assertEquals("3", new String(e2.get("x".getBytes(), ReadOptions.DEFAULT).value()));
        assertNull(e2.get("y".getBytes(), ReadOptions.DEFAULT));
    }
}
