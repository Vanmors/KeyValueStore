package dev.kvstore.codec;

import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import dev.kvstore.core.model.ValueRecord;
import dev.kvstore.proto.KVRecord;
import org.junit.jupiter.api.Test;
import org.opentest4j.TestAbortedException;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.util.*;
import java.util.zip.GZIPInputStream;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Сравнение трёх кодеков на реальном датасете Wikipedia Pageviews:
 * <a href="https://dumps.wikimedia.org/other/pageviews/2025/2025-01/" target="_blank">
 * dumps.wikimedia.org/other/pageviews/2025/2025-01/
 * </a>
 * <p>
 * CSV: target/codec_report.csv
 * </p>
 * Запуск:
 * -Dpageviews=/abs/path/to/file[.gz]
 * -Dpageviews.max=2147483647 -Dpageviews.seed=1357
 */

public class ProtoCompareCsvTest {

    private static final int MAX_ROWS = Integer.getInteger("pageviews.max", Integer.MAX_VALUE);
    private static final int SEED = Integer.getInteger("pageviews.seed", 1357);
    private static final Locale L = Locale.US;

    /* модель тестовой строки */
    record Row(byte[] key, ValueRecord vr, boolean compressed) {
    }

    private static Path resolveDataset() {
        // -Dpageviews
        String prop = System.getProperty("pageviews");
        if (prop != null && !prop.isBlank()) {
            Path p = Paths.get(prop);
            if (Files.isRegularFile(p)) return p;
        }
        return null;
    }

    /**
     * Открыть файл, распознав GZIP по сигнатуре 1F 8B.
     */
    private static InputStream openMaybeGzip(Path file) throws IOException {
        BufferedInputStream raw = new BufferedInputStream(Files.newInputStream(file), 1 << 20);
        raw.mark(2);
        int b1 = raw.read(), b2 = raw.read();
        raw.reset();
        return (b1 == 0x1f && b2 == 0x8b) ? new GZIPInputStream(raw, 1 << 20) : raw;
    }

    /**
     * Лексикографическая сортировка по byte[], без аллокаций String.
     */
    private static final Comparator<Row> KEY_CMP = (a, b) -> {
        byte[] x = a.key(), y = b.key();
        int n = Math.min(x.length, y.length);
        for (int i = 0; i < n; i++) {
            int d = (x[i] & 0xFF) - (y[i] & 0xFF);
            if (d != 0) return d;
        }
        return Integer.compare(x.length, y.length);
    };

    /**
     * Чтение pageviews: ключ = "<project>/<page>/%08d", payload = 16B (views,bytes) LE.
     */
    private static List<Row> readPageviews(Path file, int max, int seed) throws IOException {
        Random rnd = new Random(seed);
        List<Row> rows = new ArrayList<>(Math.min(max, 200_000));

        int skipped = 0, badNum = 0, i = 0;

        try (InputStream in = openMaybeGzip(file);
             BufferedReader br = new BufferedReader(new InputStreamReader(in, StandardCharsets.UTF_8), 1 << 20)) {
            String line;
            while ((line = br.readLine()) != null && rows.size() < max) {
                if (line.isEmpty() || line.charAt(0) == '#') {
                    skipped++;
                    continue;
                }
                String[] p = line.split("\\s+");
                if (p.length < 4) {
                    skipped++;
                    continue;
                }

                String project = p[0];
                String page = p[1];
                long views, bytes;
                try {
                    views = Long.parseLong(p[2]);
                    bytes = Long.parseLong(p[3]);
                } catch (NumberFormatException e) {
                    badNum++;
                    continue;
                }

                // ключ
                byte[] key = (project + "/" + page + "/" + String.format("%08d", i++))
                        .getBytes(StandardCharsets.UTF_8);

                // payload=16 байт
                ByteBuffer vb = ByteBuffer.allocate(16).order(ByteOrder.LITTLE_ENDIAN);
                vb.putLong(views).putLong(bytes);
                byte[] val = vb.array();

                // версии/TTL
                long baseV = (rows.size() / 512) * 100_000L;
                long ver = baseV + rnd.nextInt(0, 64);
                Long ttl = (rnd.nextInt(100) < 5) ? (1_700_000_000_000L + rnd.nextInt(0, 60_000)) : null;

                rows.add(new Row(key, new ValueRecord(val, ver, ttl), false));
            }
        }

        rows.sort(KEY_CMP);

        System.out.printf(L, "[dataset] file=%s%n", file.toAbsolutePath());
        System.out.printf(L, "[dataset] parsed=%d, skipped=%d, bad_numbers=%d, limit=%s%n",
                rows.size(), skipped, badNum, (max == Integer.MAX_VALUE ? "∞" : String.valueOf(max)));
        if (!rows.isEmpty()) {
            System.out.printf(L, "[dataset] firstKey='%s', firstPayloadLen=%dB%n",
                    new String(rows.get(0).key(), StandardCharsets.UTF_8),
                    rows.get(0).vr().value().length);
        }
        return rows;
    }

    /* encode/decode */

    private static byte[] encValueCodec(ValueRecord vr, long baseV, Long baseE, boolean compressed) {
        ByteBuffer buf = ByteBuffer.allocate(32 + vr.value().length);
        ValueCodec.write(vr, baseV, baseE, buf, vr.value(), compressed);
        byte[] out = new byte[buf.position()];
        buf.flip();
        buf.get(out);
        return out;
    }

    private static ValueRecord decValueCodec(byte[] bytes, long baseV, Long baseE) {
        return ValueCodec.read(ByteBuffer.wrap(bytes), baseV, baseE);
    }

    private static byte[] encProtoLike(ValueRecord vr, boolean compressed) {
        ByteBuffer buf = ByteBuffer.allocate(32 + vr.value().length);
        ProtoLikeValueCodec.write(vr, buf, compressed);
        byte[] out = new byte[buf.position()];
        buf.flip();
        buf.get(out);
        return out;
    }

    private static ValueRecord decProtoLike(byte[] bytes) {
        return ProtoLikeValueCodec.read(ByteBuffer.wrap(bytes)).vr();
    }

    private static byte[] encProtobuf(ValueRecord vr, boolean compressed) {
        KVRecord.Builder b = KVRecord.newBuilder()
                .setVersion(vr.version())
                .setValue(ByteString.copyFrom(vr.value()));
        if (vr.expireAtMillis() != null) b.setExpireAt(vr.expireAtMillis());
        if (compressed) b.setCompressed(true);
        return b.build().toByteArray();
    }

    private static ValueRecord decProtobuf(byte[] bytes) {
        try {
            KVRecord rec = KVRecord.parseFrom(bytes);
            Long ttl = rec.hasExpireAt() ? rec.getExpireAt() : null;
            return new ValueRecord(rec.getValue().toByteArray(), rec.getVersion(), ttl);
        } catch (InvalidProtocolBufferException e) {
            throw new RuntimeException(e);
        }
    }

    /* метрики */

    private static void gcQuiet() {
        try {
            System.gc();
            Thread.sleep(50);
        } catch (InterruptedException ignored) {
        }
    }

    private static long usedHeapMB() {
        gcQuiet();
        Runtime r = Runtime.getRuntime();
        return (r.totalMemory() - r.freeMemory()) / (1024 * 1024);
    }

    private record Metrics(int bytes, double encMs, double decMs, long checksum, long heapMB) {
    }

    private interface Enc {
        byte[] e(Row r);
    }

    private interface Dec {
        ValueRecord d(byte[] b);
    }

    /**
     * Стримовый прогон без хранения всех byte[] чтобы было меньше пиковых аллокаций
     */
    private static Metrics runStreamed(String name, List<Row> rows, Enc enc, Dec dec) {
        // Прогрев
        int warm = Math.min(10000, rows.size());
        for (int i = 0; i < warm; i++) dec.d(enc.e(rows.get(i)));

        long heapBefore = usedHeapMB();

        long t0 = System.nanoTime();
        int totalBytes = 0;
        // слегка держим буферы, чтобы не было нулевых аллокаций в encode
        List<byte[]> ring = new ArrayList<>(8192);
        for (Row r : rows) {
            byte[] b = enc.e(r);
            totalBytes += b.length;
            ring.add(b);
            if (ring.size() == 8192) ring.clear();
        }
        long t1 = System.nanoTime();
        double encMs = (t1 - t0) / 1e6;

        long decNanos = 0L;
        long sum = 0L;
        for (Row r : rows) {
            // готовим байты ВНЕ таймера
            byte[] b = enc.e(r);

            long tBefore = System.nanoTime();
            ValueRecord vr = dec.d(b);
            long tAfter = System.nanoTime();

            decNanos += (tAfter - tBefore);
            sum += vr.version() ^ (long) vr.value().length;
        }
        double decMs = decNanos / 1e6;

        long heapAfter = usedHeapMB();

        System.out.printf(L,
                "[%s] n=%d, bytes=%d, encode=%.3f ms, decode=%.3f ms, heap~%dMB -> %dMB%n",
                name, rows.size(), totalBytes, encMs, decMs, heapBefore, heapAfter);

        return new Metrics(totalBytes, encMs, decMs, sum, heapAfter);
    }

    private static List<Row> takeFraction(List<Row> all, double fraction) {
        int n = (int) Math.max(1, Math.floor(all.size() * fraction));
        return all.subList(0, n);
    }

    private static double mbps(int bytes, double ms) {
        if (ms <= 0) return Double.POSITIVE_INFINITY;
        return (bytes / (1024.0 * 1024.0)) / (ms / 1000.0);
    }

    @Test
    void dataset_only_to_csv_with_sizes_100_50_25_and_memory() throws Exception {
        Path pv = resolveDataset();
        if (pv == null) {
            throw new TestAbortedException(
                    "Dataset not found.");
        }

        var all = readPageviews(pv, MAX_ROWS, SEED);
        assertFalse(all.isEmpty(), "Датасет пуст или не распарсился");
        System.out.printf(L, "[dataset] totalRows=%d%n", all.size());

        Map<String, List<Row>> profiles = new LinkedHashMap<>();
        profiles.put("100pct", all);
        profiles.put("50pct", takeFraction(all, 0.50));
        profiles.put("25pct", takeFraction(all, 0.25));

        long baseV = all.get(0).vr().version();
        Long baseE = all.stream().map(r -> r.vr().expireAtMillis()).filter(Objects::nonNull).findFirst().orElse(0L);

        Path out = Paths.get("target/codec_report.csv");
        Files.createDirectories(out.getParent());

        try (BufferedWriter w = Files.newBufferedWriter(out, StandardCharsets.UTF_8)) {
            w.write("profile,n,codec,bytes,encode_ms,decode_ms,enc_mbs,dec_mbs,checksum,heap_mb,saving_vs_protobuf\n");

            for (var e : profiles.entrySet()) {
                String profile = e.getKey();
                List<Row> rows = e.getValue();

                // сначала protobuf
                Metrics pb = runStreamed("protobuf/" + profile, rows,
                        r -> encProtobuf(r.vr(), r.compressed),
                        ProtoCompareCsvTest::decProtobuf);

                // затем proto-like
                Metrics pl = runStreamed("proto-like/" + profile, rows,
                        r -> encProtoLike(r.vr(), r.compressed),
                        ProtoCompareCsvTest::decProtoLike);

                // и delta-кодек v1
                Metrics od = runStreamed("our-delta/" + profile, rows,
                        r -> encValueCodec(r.vr(), baseV, baseE, r.compressed),
                        b -> decValueCodec(b, baseV, baseE));

                double pbEncMBs = mbps(pb.bytes, pb.encMs), pbDecMBs = mbps(pb.bytes, pb.decMs);
                double plEncMBs = mbps(pl.bytes, pl.encMs), plDecMBs = mbps(pl.bytes, pl.decMs);
                double odEncMBs = mbps(od.bytes, od.encMs), odDecMBs = mbps(od.bytes, od.decMs);

                w.write(String.format(L, "%s,%d,%s,%d,%.3f,%.3f,%.3f,%.3f,%d,%d,%.2f%n",
                        profile, rows.size(), "protobuf", pb.bytes, pb.encMs, pb.decMs,
                        pbEncMBs, pbDecMBs, pb.checksum, pb.heapMB, 0.0));

                double plSave = 100.0 * (pb.bytes - pl.bytes) / pb.bytes;
                w.write(String.format(L, "%s,%d,%s,%d,%.3f,%.3f,%.3f,%.3f,%d,%d,%.2f%n",
                        profile, rows.size(), "proto-like", pl.bytes, pl.encMs, pl.decMs,
                        plEncMBs, plDecMBs, pl.checksum, pl.heapMB, plSave));

                double odSave = 100.0 * (pb.bytes - od.bytes) / pb.bytes;
                w.write(String.format(L, "%s,%d,%s,%d,%.3f,%.3f,%.3f,%.3f,%d,%d,%.2f%n",
                        profile, rows.size(), "our-delta", od.bytes, od.encMs, od.decMs,
                        odEncMBs, odDecMBs, od.checksum, od.heapMB, odSave));
            }
        }

        System.out.println("CSV written: " + out.toAbsolutePath());
        assertTrue(Files.size(out) > 0, "CSV не создался или пустой");
    }
}
