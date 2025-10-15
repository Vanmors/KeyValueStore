package dev.kvstore.codec;

import dev.kvstore.core.model.ValueRecord;

import java.nio.ByteBuffer;

public final class ProtoLikeValueCodec {
    private ProtoLikeValueCodec() {
    }

    // wire types
    private static final int WT_VARINT = 0;
    private static final int WT_LEN = 2;

    // field numbers
    private static final int F_VERSION = 1;
    private static final int F_EXPIRE_AT = 2;
    private static final int F_VALUE = 3;
    private static final int F_COMPRESSED = 4;

    private static int tag(int field, int wt) {
        return (field << 3) | wt;
    }

    public static void write(ValueRecord vr, ByteBuffer dst, boolean compressed) {
        // version (sint64 as zigzag varint)
        VarInts.putVarInt(tag(F_VERSION, WT_VARINT), dst);
        VarInts.putVarLong(ZigZag.encode(vr.version()), dst);

        // expire_at (optional)
        if (vr.expireAtMillis() != null) {
            VarInts.putVarInt(tag(F_EXPIRE_AT, WT_VARINT), dst);
            VarInts.putVarLong(ZigZag.encode(vr.expireAtMillis()), dst);
        }

        // value (bytes, length-delimited)
        VarInts.putVarInt(tag(F_VALUE, WT_LEN), dst);
        VarInts.putVarInt(vr.value().length, dst);
        dst.put(vr.value());

        // compressed flag (optional, write only if true)
        if (compressed) {
            VarInts.putVarInt(tag(F_COMPRESSED, WT_VARINT), dst);
            VarInts.putVarInt(1, dst); // bool true
        }
    }

    public static Decoded read(ByteBuffer src) {
        Long version = null, expire = null;
        boolean compressed = false;
        byte[] value = null;

        while (src.hasRemaining()) {
            int t = VarInts.getVarInt(src); // tag
            if (t == 0) break; // defensive break (protobuf allows 0 to end stream)
            int wt = t & 0x07;
            int field = t >>> 3;

            switch (field) {
                case F_VERSION -> {
                    if (wt != WT_VARINT) throw new CodecFormatException("version: wire type mismatch");
                    version = ZigZag.decode(VarInts.getVarLong(src));
                }
                case F_EXPIRE_AT -> {
                    if (wt != WT_VARINT) throw new CodecFormatException("expireAt: wire type mismatch");
                    expire = ZigZag.decode(VarInts.getVarLong(src));
                }
                case F_VALUE -> {
                    if (wt != WT_LEN) throw new CodecFormatException("value: wire type mismatch");
                    int len = VarInts.getVarInt(src);
                    if (len < 0 || len > 64 * 1024 * 1024)
                        throw new CodecFormatException("value length out of bounds: " + len);
                    if (src.remaining() < len) throw new CodecFormatException("truncated value");
                    value = new byte[len];
                    src.get(value);
                }
                case F_COMPRESSED -> {
                    if (wt != WT_VARINT) throw new CodecFormatException("compressed: wire type mismatch");
                    compressed = VarInts.getVarInt(src) != 0;
                }
                default -> {
                    // пропуск незнакомого поля:
                    skipUnknown(src, wt);
                }
            }
        }
        if (version == null || value == null) throw new CodecFormatException("missing required fields: version/value");
        return new Decoded(new ValueRecord(value, version, expire), compressed);
    }

    private static void skipUnknown(ByteBuffer src, int wt) {
        switch (wt) {
            case WT_VARINT -> VarInts.getVarLong(src);
            case WT_LEN -> {
                int len = VarInts.getVarInt(src);
                if (len < 0 || src.remaining() < len) throw new CodecFormatException("truncated len-delimited");
                src.position(src.position() + len);
            }
            default -> throw new CodecFormatException("unsupported wire type: " + wt);
        }
    }

    public record Decoded(ValueRecord vr, boolean compressed) {
    }

    public static final class CodecFormatException extends RuntimeException {
        public CodecFormatException(String m) {
            super(m);
        }
    }
}
