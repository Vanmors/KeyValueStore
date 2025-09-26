package dev.kvstore.codec;

import dev.kvstore.core.model.ValueRecord;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.util.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Проверяем, что блок с префиксным сжатием ключей занимает меньше места, чем простой baseline-блок
 */
public class BlockBuilderTest {
    static class BaselineValueCodec {
        // version(long) + expireAt(long or -1) + valueLen(int) + value
        static void write(ValueRecord vr, ByteBuffer dst) {
            dst.putLong(vr.version());
            dst.putLong(vr.expireAtMillis() == null ? -1L : vr.expireAtMillis());
            dst.putInt(vr.value().length);
            dst.put(vr.value());
        }
    }

    static class BaselineBlockBuilder {
        private final ByteBuffer buf;
        private int entries;

        BaselineBlockBuilder(ByteBuffer buf) {
            this.buf = buf;
        }

        void add(byte[] key, ByteBuffer baselineValueSlice) {
            buf.putInt(key.length);
            buf.put(key);
            buf.putInt(baselineValueSlice.remaining());
            buf.put(baselineValueSlice.duplicate());
            entries++;
        }

        void finish() {
            buf.putInt(entries);
        }
    }

    static class Row {
        final byte[] key;
        final ValueRecord vr;

        Row(byte[] k, ValueRecord v) {
            this.key = k;
            this.vr = v;
        }
    }

    /**
     * Генерируем отсортированный набор (общий префикс ключей «tenantX/tableY/»),
     * смешиваем малые (1..15) и большие (200..4096) значения.
     */
    private static List<Row> dataset() {
        Random rnd = new Random(777);
        ArrayList<Row> rows = new ArrayList<>(5000);
        for (int i = 0; i < 5000; i++) {
            String prefix = "tenant" + (i % 16) + "/table" + (i % 8) + "/";
            String keyStr = prefix + String.format("%08d", i);
            byte[] key = keyStr.getBytes();

            boolean small = rnd.nextInt(100) < 40;
            int valLen = small ? rnd.nextInt(1, 16) : rnd.nextInt(200, 4096);
            byte[] val = new byte[valLen];
            rnd.nextBytes(val);

            long baseV = (i / 64) * 10_000L; // базовые версии «батчами»
            long ver = baseV + rnd.nextInt(0, 32);
            Long ttl = (rnd.nextInt(100) < 30) ? (1_700_000_000_000L + rnd.nextInt(0, 60_000)) : null;

            rows.add(new Row(key, new ValueRecord(val, ver, ttl)));
        }
        rows.sort(Comparator.comparing(r -> new String(r.key)));
        return rows;
    }

    private static int sharedPrefixLen(byte[] a, byte[] b) {
        int i = 0, n = Math.min(a.length, b.length);
        while (i < n && a[i] == b[i]) i++;
        return i;
    }

    private static String fmtBytes(int bytes) {
        if (bytes < 1024) return bytes + " B";
        double kb = bytes / 1024.0;
        if (kb < 1024) return String.format("%.2f KiB", kb);
        double mb = kb / 1024.0;
        return String.format("%.2f MiB", mb);
    }

    @Test
    void compareSizes_block_vs_baseline() {
        final int N = 5_000;
        final int RESTART_INTERVAL = 16; // чем больше интервал - тем меньше метаданных

        List<Row> rows = dataset();

        ByteBuffer bufBlock = ByteBuffer.allocate(10_000_000);
        BlockBuilder bb = new BlockBuilder(bufBlock, RESTART_INTERVAL);
        bb.blockBaseVersion = rows.get(0).vr.version();
        bb.blockBaseExpire = rows.stream()
                .map(r -> r.vr.expireAtMillis())
                .filter(Objects::nonNull)
                .findFirst()
                .orElse(0L);

        int smallCount = 0;
        long totalKeyLen = 0;
        long totalShared = 0;
        byte[] prevKey = new byte[0];

        for (Row r : rows) {
            if (r.vr.value().length <= 15) smallCount++;

            totalKeyLen += r.key.length;
            totalShared += sharedPrefixLen(prevKey, r.key);
            prevKey = r.key;

            ByteBuffer tmp = ByteBuffer.allocate(8 + 8 + 4 + r.vr.value().length);
            ValueCodec.write(r.vr, bb.blockBaseVersion, bb.blockBaseExpire, tmp, r.vr.value(), false);
            tmp.flip();
            bb.add(r.key, tmp);
        }
        bb.finish();
        int ourSize = bufBlock.position();

        ByteBuffer bufBase = ByteBuffer.allocate(10_000_000);
        BaselineBlockBuilder base = new BaselineBlockBuilder(bufBase);
        for (Row r : rows) {
            ByteBuffer tmp = ByteBuffer.allocate(8 + 8 + 4 + r.vr.value().length);
            BaselineValueCodec.write(r.vr, tmp);
            tmp.flip();
            base.add(r.key, tmp);
        }
        base.finish();
        int baseSize = bufBase.position();

        double savingPct = 100.0 * (baseSize - ourSize) / baseSize;
        double avgKey = totalKeyLen / (double) N;
        double avgShared = totalShared / (double) (N - 1);
        int expectedRestarts = (N + RESTART_INTERVAL - 1) / RESTART_INTERVAL; // оценка

        System.out.printf(
                """
                        entries=%d, restartInterval=%d, expectedRestarts≈%d
                        keys: avgLen=%.2f B, avgSharedWithPrev=%.2f B
                        values: small(<=15B)=%d (%.1f%%)
                        
                        BASELINE block size: %s  (%d bytes)
                        BLOCK    block size: %s  (%d bytes)
                        Saving:  %.2f%%
                        """,
                N, RESTART_INTERVAL, expectedRestarts,
                avgKey, avgShared,
                smallCount, (100.0 * smallCount / N),
                fmtBytes(baseSize), baseSize,
                fmtBytes(ourSize), ourSize,
                savingPct
        );

        assertTrue(ourSize < baseSize, "Ожидали экономию размера блока относительно baseline");
    }
}
