package dev.kvstore.codec;

import dev.kvstore.core.model.ValueRecord;

import java.nio.ByteBuffer;

/**
 * ValueRecord для компактной записи/чтения байтового представления.
 * <p> Первый байт — флаги: *
 * <li>{@code FLAG_TTL (1)} — присутствует TTL
 * <li>{@code FLAG_COMP (1<<1)} — полезная нагрузка сжата
 * <li>{@code FLAG_INLINE (1<<3)} — малое значение <= 15 байт и его длина
 * хранится в 4 младших битах следующего байта</li>
 */
public final class ValueCodec {
    private static final int FLAG_TTL = 1;       // 0001
    private static final int FLAG_COMP = 1 << 1;  // 0010
    // Бит 2 зарезервирован под что-то (сейчас не используется)
    private static final int FLAG_INLINE = 1 << 3;  // 1000 (в младших 4 битах)

    private static final int INLINE_MAX = 15;

    private ValueCodec() {
    }

    public static void write(ValueRecord vr,
                             long blockBaseVersion,
                             Long blockBaseExpire,
                             ByteBuffer dst,
                             byte[] payload,
                             boolean compressed) {
        int flags = 0;
        if (vr.expireAtMillis() != null) flags |= FLAG_TTL;
        if (compressed) flags |= FLAG_COMP;

        final int len = payload.length;
        if (len <= INLINE_MAX) {
            flags |= FLAG_INLINE;
            // запишем длину в старшие 4 бита
            flags |= (len & 0x0F) << 4;
        }

        // byte0: флаги + inline-длина
        dst.put((byte) flags);

        // версия (дельта к base) ZigZag+varint
        long vDelta = vr.version() - blockBaseVersion;
        VarInts.putVarLong(ZigZag.encode(vDelta), dst);

        // TTL дельта
        if ((flags & FLAG_TTL) != 0) {
            long base = blockBaseExpire != null ? blockBaseExpire : 0L;
            long tDelta = vr.expireAtMillis() - base;
            VarInts.putVarLong(ZigZag.encode(tDelta), dst);
        }

        // payload
        if ((flags & FLAG_INLINE) != 0) {
            // длина уже в byte0, сразу пишем байты
            dst.put(payload);
        } else {
            VarInts.putVarInt(len, dst);
            dst.put(payload);
        }
    }

    public static ValueRecord read(ByteBuffer src,
                                   long blockBaseVersion,
                                   Long blockBaseExpire) {
        int head = Byte.toUnsignedInt(src.get());
        int flags = head & 0x0F;          // младшие 4 бита — флаги
        int inLen4 = (head >>> 4) & 0x0F;  // старшие 4 бита — inline длина

        long vDelta = ZigZag.decode(VarInts.getVarLong(src));
        long version = blockBaseVersion + vDelta;

        Long expire = null;
        if ((flags & FLAG_TTL) != 0) {
            long base = blockBaseExpire != null ? blockBaseExpire : 0L;
            long tDelta = ZigZag.decode(VarInts.getVarLong(src));
            expire = base + tDelta;
        }

        int len;
        if ((flags & FLAG_INLINE) != 0) {
            len = inLen4; // длина уже в заголовке
        } else {
            len = VarInts.getVarInt(src);
        }

        byte[] val = new byte[len];
        src.get(val);
        return new ValueRecord(val, version, expire);
    }
}