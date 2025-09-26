package dev.kvstore.codec;

import dev.kvstore.core.model.ValueRecord;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.util.Random;

import static org.junit.jupiter.api.Assertions.*;

public class ValueCodecTest {
    private static byte[] bytes(int n, int seed) {
        byte[] b = new byte[n];
        new Random(seed).nextBytes(b);
        return b;
    }

    private static int encodedSize(ValueRecord vr, long baseV, byte[] payload) {
        ByteBuffer buf = ByteBuffer.allocate(32 + payload.length);
        ValueCodec.write(vr, baseV, null, buf, payload, false);
        return buf.position();
    }

    private static int encodedLenInt(int x) {
        ByteBuffer buf = ByteBuffer.allocate(5);
        VarInts.putVarInt(x, buf);
        return buf.position();
    }

    /**
     * Inline (≤15B), без TTL: раунд-трип и маленький overhead.
     */
    @Test
    void encodeDecode_inline_noTtl_ok() {
        byte[] v = bytes(12, 1);
        long baseV = 100L;
        ValueRecord vr = new ValueRecord(v, baseV + 5, null);

        ByteBuffer buf = ByteBuffer.allocate(64);
        ValueCodec.write(vr, baseV, null, buf, v, false);
        int total = buf.position();
        int overhead = total - v.length;

        int verVar = VarInts.sizeOfVarLong(ZigZag.encode(vr.version() - baseV));
        System.out.printf("[inline] payload=%dB total=%dB overhead=%dB verDelta=%d(var=%dB)%n",
                v.length, total, overhead, (vr.version() - baseV), verVar);

        buf.flip();
        ValueRecord vr2 = ValueCodec.read(buf, baseV, null);

        assertArrayEquals(v, vr2.value());
        assertEquals(vr.version(), vr2.version());
        assertNull(vr2.expireAtMillis());
        assertTrue(overhead <= 6, "слишком большой overhead для inline");
    }

    /**
     * Большое значение с TTL: раунд-трип и адекватные накладные.
     */
    @Test
    void encodeDecode_large_withTtl_ok() {
        byte[] v = bytes(10_000, 2);
        long baseV = 1_000_000L;
        long baseE = 1_700_000_000_000L;
        ValueRecord vr = new ValueRecord(v, baseV + 7, baseE + 5_000);

        ByteBuffer buf = ByteBuffer.allocate(11_000);
        ValueCodec.write(vr, baseV, baseE, buf, v, false);
        int total = buf.position();
        int overhead = total - v.length;

        int verVar = VarInts.sizeOfVarLong(ZigZag.encode(vr.version() - baseV));
        int ttlVar = VarInts.sizeOfVarLong(ZigZag.encode(vr.expireAtMillis() - baseE));
        System.out.printf("[large+ttl] payload=%dB total=%dB overhead=%dB verΔ=%d(var=%dB) ttlΔ=%d(var=%dB)%n",
                v.length, total, overhead,
                (vr.version() - baseV), verVar,
                (vr.expireAtMillis() - baseE), ttlVar);

        buf.flip();
        ValueRecord vr2 = ValueCodec.read(buf, baseV, baseE);

        assertArrayEquals(v, vr2.value());
        assertEquals(vr.version(), vr2.version());
        assertEquals(vr.expireAtMillis(), vr2.expireAtMillis());
        assertTrue(overhead < 40, "неожиданно большие накладные");
    }

    /**
     * Порог инлайна: 15B кодируется короче 16B.
     */
    @Test
    void inlineThreshold_15vs16_smaller() {
        long baseV = 10L;

        byte[] v15 = bytes(15, 15);
        byte[] v16 = bytes(16, 16);
        int s15 = encodedSize(new ValueRecord(v15, baseV + 1, null), baseV, v15);
        int s16 = encodedSize(new ValueRecord(v16, baseV + 1, null), baseV, v16);
        int oh15 = s15 - v15.length;
        int oh16 = s16 - v16.length;

        System.out.printf("[inline-threshold] 15B=%dB(oh=%d) 16B=%dB(oh=%d) diff=%dB%n",
                s15, oh15, s16, oh16, (s16 - s15));

        assertTrue(s15 < s16, "15B должно быть короче 16B");
    }


    /**
     * sizeOfVarInt: граничные значения 1..5 байт.
     */
    @Test
    void sizeOfVarInt_boundaries_ok() {
        // 1 байт
        assertEquals(1, VarInts.sizeOfVarInt(0));
        assertEquals(1, VarInts.sizeOfVarInt(0x7F));
        assertEquals(2, VarInts.sizeOfVarInt(0x80));
        // 2 байта
        assertEquals(2, VarInts.sizeOfVarInt(0x3FFF));
        assertEquals(3, VarInts.sizeOfVarInt(0x4000));
        // 3 байта
        assertEquals(3, VarInts.sizeOfVarInt(0x1FFFFF));
        assertEquals(4, VarInts.sizeOfVarInt(0x200000));
        // 4 байта
        assertEquals(4, VarInts.sizeOfVarInt(0x0FFFFFFF));
        assertEquals(5, VarInts.sizeOfVarInt(0x10000000));
        // 5 байт
        assertEquals(5, VarInts.sizeOfVarInt(0x7FFFFFFF));
        assertEquals(5, VarInts.sizeOfVarInt(0x80000000));
        assertEquals(5, VarInts.sizeOfVarInt(0xFFFFFFFF));
    }

    /**
     * sizeOfVarInt совпадает с фактической длиной (random 10k).
     */
    @Test
    void sizeOfVarInt_matchesEncodedLen_random_ok() {
        Random rnd = new Random(2025);
        int[] buckets = new int[5];
        long total = 0;

        for (int i = 0; i < 10_000; i++) {
            int x = rnd.nextInt();
            int reported = VarInts.sizeOfVarInt(x);
            int actual = encodedLenInt(x);

            assertTrue(reported >= 1 && reported <= 5);
            assertEquals(reported, actual);

            buckets[reported - 1]++;
            total += reported;
        }
        double avg = total / 10_000.0;
        System.out.printf("[sizeOfVarInt] avg=%.3fB dist={1b:%d 2b:%d 3b:%d 4b:%d 5b:%d}%n",
                avg, buckets[0], buckets[1], buckets[2], buckets[3], buckets[4]);
    }

    /**
     * Распределение для неотрицательных значений длины/смещения.
     */
    @Test
    void sizeOfVarInt_nonNegative_stats() {
        Random rnd = new Random(2025);
        int[] buckets = new int[5];
        for (int i = 0; i < 100_000; i++) {
            int x = rnd.nextInt(1 << 30); // [0 .. 2^30)
            buckets[VarInts.sizeOfVarInt(x) - 1]++;
        }
        System.out.printf("[non-negative] dist={1b:%d 2b:%d 3b:%d 4b:%d 5b:%d}%n",
                buckets[0], buckets[1], buckets[2], buckets[3], buckets[4]);
    }

    /**
     * Распределение размеров ZigZag(varint) для небольших дельт.
     */
    @Test
    void sizeOfZigZag_smallDeltas_stats() {
        Random rnd = new Random(2025);
        int[] buckets = new int[10];
        for (int i = 0; i < 100_000; i++) {
            long delta = rnd.nextInt(65) - 32; // [-32..32]
            int sz = VarInts.sizeOfVarLong(ZigZag.encode(delta));
            buckets[sz - 1]++;
        }
        System.out.printf("[zigzag] dist={1b:%d 2b:%d 3b:%d 4b:%d 5b:%d}%n",
                buckets[0], buckets[1], buckets[2], buckets[3], buckets[4]);
    }
}