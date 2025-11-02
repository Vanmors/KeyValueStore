//package dev.kvstore.core;
//
//import org.junit.jupiter.api.Test;
//
//import java.nio.file.Files;
//import java.util.List;
//
//import static org.junit.jupiter.api.Assertions.*;
//
//public class LSMEngineWALRecoveryTest {
//
//    @Test
//    void recoverLatestStateFromWAL() throws Exception {
//        var tmp = Files.createTempDirectory("lsm-wal-");
//
//        var kv1 = new KeyValueStoreImpl(tmp.toString(), 8_192, "master", List.of());
//        kv1.put("x".getBytes(), "1".getBytes());
//        kv1.put("y".getBytes(), "2".getBytes());
//        kv1.put("x".getBytes(), "3".getBytes());
//        kv1.delete("y".getBytes());
//
//        var kv2 = new KeyValueStoreImpl(tmp.toString(), 8_192, "master", List.of());
//
//        assertEquals("3", new String(kv2.get("x".getBytes()).value().value()));
//        assertNull(kv2.get("y".getBytes()).value().value());
//    }
//}
