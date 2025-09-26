package dev.kvstore.codec;

import org.junit.jupiter.api.Test;

import java.nio.BufferUnderflowException;
import java.nio.ByteBuffer;
import java.util.Random;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Проверяем:
 * - decode(encode(x)) == x на большом количестве случайных long
 * - read(write(x)) == x на 100k случайных значений
 * - распределение фактических длин varint
 * - корректная ошибка для слишком длинного и малого varint
 */
public class VarIntZigZagTest {

    @Test
    void zigzagRoundTripRandomLongs() {
        Random rnd = new Random(123);
        for (long x : new long[]{0, -1, 1}) {
            long enc = ZigZag.encode(x);
            long dec = ZigZag.decode(enc);
            System.out.printf("[ZigZag sample] x=%d -> enc=%d -> dec=%d%n", x, enc, dec);
            assertEquals(x, dec);
            // младший бит ZigZag-кода = знак x (1 если x<0, иначе 0)
            int signBit = (x < 0) ? 1 : 0;
            assertEquals(signBit, (int) (enc & 1L));
        }

        for (int i = 0; i < 100_000; i++) {
            long x = rnd.nextLong();
            long enc = ZigZag.encode(x);
            long dec = ZigZag.decode(enc);
            assertEquals(x, dec, "ZigZag decode(encode(x)) нарушено");
            int signBit = (x < 0) ? 1 : 0;
            assertEquals(signBit, (int) (enc & 1L), "LSB ZigZag-кода должен отражать знак");
        }
    }

    @Test
    void varIntRoundTripRandom_withLengthStats() {
        Random rnd = new Random(42);
        ByteBuffer buf = ByteBuffer.allocate(10); // int максимум 5 байт
        int[] buckets = new int[5];
        long totalBytes = 0;

        for (int i = 0; i < 100_000; i++) {
            int x = rnd.nextInt();
            buf.clear();
            VarInts.putVarInt(x, buf);
            int len = buf.position(); // сколько реально байт занял varint
            if (len >= 1 && len <= 5) buckets[len - 1]++;
            totalBytes += len;

            buf.flip();
            int y = VarInts.getVarInt(buf);
            assertEquals(x, y, "varint(int) раунд-трип нарушен");
        }

        double avg = totalBytes / 100_000.0;
        System.out.printf("[VarInt stats] avgLen=%.3f bytes, dist={1b:%d, 2b:%d, 3b:%d, 4b:%d, 5b:%d}%n",
                avg, buckets[0], buckets[1], buckets[2], buckets[3], buckets[4]);
    }

    @Test
    void varLongRoundTripRandom_withLengthStats() {
        Random rnd = new Random(43);
        ByteBuffer buf = ByteBuffer.allocate(16); // long максимум 10 байт
        int[] buckets = new int[10];
        long totalBytes = 0;

        for (int i = 0; i < 100_000; i++) {
            long x = rnd.nextLong();
            buf.clear();
            VarInts.putVarLong(x, buf);
            int len = buf.position();
            if (len >= 1 && len <= 10) buckets[len - 1]++;
            totalBytes += len;

            buf.flip();
            long y = VarInts.getVarLong(buf);
            assertEquals(x, y, "varint(long) раунд-трип нарушен");
        }

        double avg = totalBytes / 100_000.0;
        System.out.printf("[VarLong stats] avgLen=%.3f bytes, dist={", avg);
        for (int i = 0; i < 10; i++) {
            System.out.printf("%db:%d%s", i + 1, buckets[i], i == 9 ? "}\n" : ", ");
        }
    }

    @Test
    void varInt_malformedTooLong_throws() {
        // 5 байт подряд с установленным битом продолжения (0x80): нет завершающего байта,
        byte[] bad = new byte[]{(byte) 0x80, (byte) 0x80, (byte) 0x80, (byte) 0x80, (byte) 0x80};
        ByteBuffer src = ByteBuffer.wrap(bad);
        assertThrows(IllegalArgumentException.class, () -> VarInts.getVarInt(src),
                "Ожидали IllegalArgumentException для varint > 5 байт");
    }

    @Test
    void varInt_truncated_throwsBufferUnderflow() {
        // один байт с битом продолжения, а дальше нет данных
        byte[] truncated = new byte[]{(byte) 0x80};
        ByteBuffer src = ByteBuffer.wrap(truncated);
        assertThrows(BufferUnderflowException.class, () -> VarInts.getVarInt(src),
                "Ожидали BufferUnderflowException для обрезанного varint");
    }
}
