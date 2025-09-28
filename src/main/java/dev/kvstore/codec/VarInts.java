package dev.kvstore.codec;

import java.nio.ByteBuffer;

/**
 * Утилиты для кодирования/декодирования беззнаковых varint.
 * Формат: младшие 7 бит в каждом байте — полезные, старший бит признак продолжения.
 * Для {@code int} максимум 5 байт, для {@code long} — 10 байт.
 */
public final class VarInts {
    private VarInts() {
    }

    public static void putVarInt(int value, ByteBuffer dst) {
        long v = Integer.toUnsignedLong(value);
        // 1 байт
        putVarLong(v, dst);
    }

    public static void putVarLong(long value, ByteBuffer dst) {
        if ((value & ~0x7FL) == 0) {
            dst.put((byte) value);
            return;
        }
        writeUnsignedVarLong(value, dst);
    }

    /**
     * Низкоуровневая запись беззнакового varint (1..10 байт).
     */
    private static void writeUnsignedVarLong(long v, ByteBuffer dst) {
        // всегда пишем минимум 1 байт
        do {
            byte b = (byte) ((v & 0x7F) | 0x80);
            v >>>= 7;
            if (v == 0) {
                // последний байт — без флага продолжения
                dst.put((byte) (b & 0x7F));
                return;
            }
            dst.put(b);
        } while (true);
    }

    public static int getVarInt(ByteBuffer src) {
        // первый байт без флага продолжения
        byte b = src.get();
        long result = b & 0x7FL;
        if (b >= 0) return (int) result;

        int shift = 7;
        for (int i = 1; i < 5; i++) {
            b = src.get();
            result |= (long) (b & 0x7F) << shift;
            if (b >= 0) return (int) result;
            shift += 7;
        }
        throw new IllegalArgumentException("Malformed varint: exceeds 5 bytes");
    }

    public static long getVarLong(ByteBuffer src) {
        // первый байт без флага продолжения
        byte b = src.get();
        long result = b & 0x7FL;
        if (b >= 0) return result;

        int shift = 7;
        for (int i = 1; i < 10; i++) {
            b = src.get(); // при нехватке данных бросит BufferUnderflowException
            result |= (long) (b & 0x7F) << shift;
            if (b >= 0) return result;
            shift += 7;
        }
        throw new IllegalArgumentException("Malformed varint: exceeds 10 bytes");
    }

//    public static int sizeOfVarInt(int value) {
//        long v = Integer.toUnsignedLong(value);
//        if ((v & ~0x7FL) == 0) return 1;
//        if ((v & ~0x3FFFL) == 0) return 2;
//        if ((v & ~0x1FFFFFL) == 0) return 3;
//        if ((v & ~0xFFFFFFFL) == 0) return 4;
//        return 5;
//    }
//
//    public static int sizeOfVarLong(long value) {
//        if ((value & ~0x7FL) == 0) return 1;
//        if ((value & ~0x3FFFL) == 0) return 2;
//        if ((value & ~0x1FFFFFL) == 0) return 3;
//        if ((value & ~0xFFFFFFFL) == 0) return 4;
//        if ((value & ~0x7FFFFFFFFL) == 0) return 5;
//        if ((value & ~0x3FFFFFFFFFFL) == 0) return 6;
//        if ((value & ~0x1FFFFFFFFFFFFL) == 0) return 7;
//        if ((value & ~0xFFFFFFFFFFFFFFL) == 0) return 8;
//        if ((value & ~0x7FFFFFFFFFFFFFFFL) == 0) return 9;
//        return 10;
//    }
}
