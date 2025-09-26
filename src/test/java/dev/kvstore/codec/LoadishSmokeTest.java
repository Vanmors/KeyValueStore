package dev.kvstore.codec;

import dev.kvstore.core.model.ValueRecord;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.util.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Замеряем:
 * - размер закодированных данных
 * - время кодирования/декодирования
 * - экономию размера относительно baseline
 */
public class LoadishSmokeTest {
    private static final int N_RECORDS_DEFAULT = 50_000;
    private static final int INITIAL_BUF_BYTES = 16 * 1024 * 1024;
    private static final int MIN_BASE_BUF_BYTES = 32 * 1024 * 1024;
    private static final int VAR_CODEC_META_OVERHEAD = 32; // оценка метаданных на запись


    record Row(byte[] key, ValueRecord vr) {
    }

    private record Stats(int sizeBytes, double encMs, double decMs, long checksum) {
    }

    /**
     * Датасет с коротким префикс ключа, payload 1..15 байт или 200..8192 байт.
     */
    private static List<Row> datasetLegacy() {
        Random rnd = new Random(1357);
        List<Row> rows = new ArrayList<>(LoadishSmokeTest.N_RECORDS_DEFAULT);
        for (int i = 0; i < LoadishSmokeTest.N_RECORDS_DEFAULT; i++) {
            String keyStr = "tenant" + (i % 32) + "/tbl" + (i % 8) + "/" + String.format("%08d", i);
            byte[] key = keyStr.getBytes();

            boolean small = rnd.nextInt(100) < 40;
            int len = small ? rnd.nextInt(1, 16) : rnd.nextInt(200, 8192);
            byte[] val = randomBytes(rnd, len);

            long baseV = (i / 128) * 100_000L;
            long ver = baseV + rnd.nextInt(0, 64);
            Long ttl = rnd.nextInt(100) < 20 ? 1_700_000_000_000L + rnd.nextInt(0, 60_000) : null;

            rows.add(new Row(key, new ValueRecord(val, ver, ttl)));
        }
        rows.sort(Comparator.comparing(r -> new String(r.key())));
        return rows;
    }

    /**
     * Датасет с длинным общим префиксом ключей.
     */
    private static List<Row> datasetProfile(String profile) {
        Random rnd = new Random(1357);
        List<Row> rows = new ArrayList<>(LoadishSmokeTest.N_RECORDS_DEFAULT);
        for (int i = 0; i < LoadishSmokeTest.N_RECORDS_DEFAULT; i++) {
            // более реалистичный длинный префикс
            String keyStr = "dc1/clusterA/tenant" + (i % 16)
                    + "/table" + (i % 8)
                    + "/part" + (i % 4)
                    + "/" + String.format("%08d", i);
            byte[] key = keyStr.getBytes();

            int len = switch (profile) {
                case "small" -> rnd.nextInt(1, 64);
                case "mixed" -> (rnd.nextInt(100) < 60) ? rnd.nextInt(1, 32) : rnd.nextInt(200, 2048);
                default /* large */ -> rnd.nextInt(200, 8192);
            };

            byte[] val = randomBytes(rnd, len);

            long baseV = (i / 128) * 100_000L;
            long ver = baseV + rnd.nextInt(0, 64);
            Long ttl = rnd.nextInt(100) < 30 ? 1_700_000_000_000L + rnd.nextInt(0, 60_000) : null;

            rows.add(new Row(key, new ValueRecord(val, ver, ttl)));
        }
        rows.sort(Comparator.comparing(r -> new String(r.key())));
        return rows;
    }

    private static byte[] randomBytes(Random rnd, int len) {
        byte[] b = new byte[len];
        rnd.nextBytes(b);
        return b;
    }

    /**
     * Кодирование нашим ValueCodec + полный проход декодирования.
     */
    private static Stats runValueCodec(List<Row> rows) {
        long baseVersion = rows.get(0).vr().version();
        Long baseExpire = rows.stream()
                .map(r -> r.vr().expireAtMillis())
                .filter(Objects::nonNull)
                .findFirst()
                .orElse(0L);

        ByteBuffer buf = ByteBuffer.allocate(INITIAL_BUF_BYTES);

        long t0 = System.nanoTime();
        for (var r : rows) {
            // безопасная верхняя граница метаданных под наш формат
            ByteBuffer tmp = ByteBuffer.allocate(VAR_CODEC_META_OVERHEAD + r.vr().value().length);
            ValueCodec.write(r.vr(), baseVersion, baseExpire, tmp, r.vr().value(), false);
            tmp.flip();

            buf = ensureCapacity(buf, tmp.remaining());
            buf.put(tmp);
        }
        int size = buf.position();
        long t1 = System.nanoTime();

        buf.flip();
        long checksum = 0;
        long t2 = System.nanoTime();
        while (buf.hasRemaining()) {
            ValueRecord vr = ValueCodec.read(buf, baseVersion, baseExpire);
            checksum += vr.version() ^ (long) vr.value().length;
        }
        long t3 = System.nanoTime();

        return new Stats(size, ms(t1 - t0), ms(t3 - t2), checksum);
    }

    /**
     * Кодирование baseline-форматом: version(long), expireAt(long/-1), len(int), bytes.
     */
    private static int runBaselineSize(List<Row> rows) {
        // приблизительно оценим требуемую ёмкость
        int estimate = rows.stream().mapToInt(r -> 8 + 8 + 4 + r.vr().value().length).sum();
        ByteBuffer base = ByteBuffer.allocate(Math.max(estimate + (1 << 20), MIN_BASE_BUF_BYTES)); // +1MiB запас

        for (var r : rows) {
            int need = 8 + 8 + 4 + r.vr().value().length;
            base = ensureCapacity(base, need);

            base.putLong(r.vr().version());
            base.putLong(r.vr().expireAtMillis() == null ? -1L : r.vr().expireAtMillis());
            base.putInt(r.vr().value().length);
            base.put(r.vr().value());
        }
        return base.position();
    }

    /**
     * Рост ByteBuffer при нехватке места. Возвращает новый (или тот же) буфер.
     */
    private static ByteBuffer ensureCapacity(ByteBuffer buf, int needMore) {
        if (buf.remaining() >= needMore) return buf;
        int newCap = Math.max(buf.capacity() * 2, buf.position() + needMore);
        ByteBuffer bigger = ByteBuffer.allocate(newCap);
        buf.flip();
        bigger.put(buf);
        return bigger;
    }

    private static double ms(long nanos) {
        return nanos / 1e6;
    }

    private static double throughputMBps(int bytes, double encMs) {
        return bytes / (encMs / 1_000.0) / (1024.0 * 1024.0);
    }

    private static String fmtBytes(int bytes) {
        if (bytes < 1024) return bytes + " B";
        double kb = bytes / 1024.0;
        if (kb < 1024) return String.format("%.2f KiB", kb);
        double mb = kb / 1024.0;
        return String.format("%.2f MiB", mb);
    }

    @Test
    void valueCodec_vs_baseline_largeProfile() {
        List<Row> rows = datasetLegacy();

        Stats s = runValueCodec(rows);
        int baseSize = runBaselineSize(rows);

        double savingPct = 100.0 * (baseSize - s.sizeBytes) / baseSize;
        double newMBps = throughputMBps(s.sizeBytes, s.encMs);

        System.out.printf("""
                        [LARGE]
                        BASELINE  size=%s
                        NEW       size=%s, encode=%.2f ms (≈%.2f MB/s), decode=%.2f ms, checksum=%d
                        Saving:   %.2f%%
                        """,
                fmtBytes(baseSize),
                fmtBytes(s.sizeBytes), s.encMs, newMBps, s.decMs, s.checksum, savingPct
        );

        assertTrue(s.sizeBytes < baseSize, "Ожидали экономию размера относительно baseline на large-профиле");
        // экономия может быть ~0–2%, это допустимо для large
        assertTrue(savingPct >= 0.0, "На больших значениях экономия может быть минимальной — это ок");
        assertTrue(s.checksum != 0, "Декод ничего не прочитал");
    }

    @Test
    void valueCodec_profiles_small_mixed_large() {

        for (String profile : List.of("small", "mixed", "large")) {
            List<Row> rows = datasetProfile(profile);

            Stats s = runValueCodec(rows);
            int baseSize = runBaselineSize(rows);

            double savingPct = 100.0 * (baseSize - s.sizeBytes) / baseSize;
            double newMBps = throughputMBps(s.sizeBytes, s.encMs);

            System.out.printf("""
                            === PROFILE: %s ===
                            BASELINE size=%s
                            NEW      size=%s, encode=%.2f ms (≈%.2f MB/s), decode=%.2f ms, checksum=%d
                            Saving:  %.2f%%
                            """,
                    profile, fmtBytes(baseSize),
                    fmtBytes(s.sizeBytes), s.encMs, newMBps, s.decMs, s.checksum,
                    savingPct
            );

            switch (profile) {
                case "small" -> assertTrue(savingPct >= 20.0, "Ожидали >=20% экономии на мелких значениях");
                case "mixed" -> assertTrue(savingPct >= 2.0, "Ожидали >=2% экономии на смешанном профиле");
                default /* large */ ->
                        assertTrue(savingPct >= 0.0, "На больших значениях экономия может быть минимальной");
            }
            assertTrue(s.checksum != 0, "Декод ничего не прочитал — подозрительно");
        }
    }
}
