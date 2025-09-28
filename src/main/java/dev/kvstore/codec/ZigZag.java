package dev.kvstore.codec;

/**
 * Кодирование ZigZag для преобразования знаковых целых в беззнаковые
 * и обратно.
 *
 * <p>Формулы:
 * <ul>
 *   <li>{@code encode(x) = (x << 1) ^ (x >> 63)}</li>
 *   <li>{@code decode(u) = (u >>> 1) ^ -(u & 1)}</li>
 * </ul>
 * <p>
 * Для {@code long} использует сдвиг вправо на 63 бита (two's complement)
 */
public final class ZigZag {
    private ZigZag() {
    }

    public static long encode(long v) {
        return (v << 1) ^ (v >> 63);
    }

    public static long decode(long v) {
        return (v >>> 1) ^ -(v & 1);
    }
}
