package dev.kvstore.debug;

import dev.kvstore.codec.VarIntsTrace;
import dev.kvstore.codec.ZigZagTrace;
import dev.kvstore.core.model.ValueRecord;

import java.nio.ByteBuffer;

public final class ProtoLikeValueCodecTrace {

    private ProtoLikeValueCodecTrace() {
    }

    // wire types
    private static final int WT_VARINT = 0;
    private static final int WT_LEN = 2;

    // field numbers (как в .proto)
    private static final int F_VERSION = 1; // sint64 (zigzag varint)
    private static final int F_EXPIRE_AT = 2; // sint64 (zigzag varint), optional
    private static final int F_VALUE = 3; // bytes (len-delimited), required
    private static final int F_COMPRESSED = 4; // bool (varint), optional

    private static int tag(int field, int wt) {
        return (field << 3) | wt;
    }

    private static String wtName(int wt) {
        return switch (wt) {
            case WT_VARINT -> "VARINT";
            case WT_LEN -> "LEN";
            default -> "UNKNOWN(" + wt + ")";
        };
    }

    // ============================== WRITE ==============================

    public static void write(ValueRecord vr, ByteBuffer dst, boolean compressed, Trace t) {
        int start = dst.position();
        t.in("PB.write",
                "version", vr.version(),
                "expireAt", vr.expireAtMillis(),
                "compressed?", compressed,
                "dst_pos_before", start);

        // --- version: field=1, wt=VARINT (sint64 via ZigZag) ---
        int tagVer = tag(F_VERSION, WT_VARINT);
        int tagPos = dst.position();
        VarIntsTrace.putVarInt(tagVer, dst, t);
        t.op("tag(version)", "field", F_VERSION, "wt", wtName(WT_VARINT),
                "tag", hex(tagVer), "pos", tagPos, "hex", hexRange(dst, tagPos, dst.position()));
        int vStart = dst.position();
        long encV = ZigZagTrace.encode(vr.version(), t);
        VarIntsTrace.putVarLong(encV, dst, t);
        t.op("value(version zigzag-varint)", "pos.from", vStart, "pos.to", dst.position(),
                "hex", hexRange(dst, vStart, dst.position()));

        // --- expireAt: optional ---
        if (vr.expireAtMillis() != null) {
            int tagExp = tag(F_EXPIRE_AT, WT_VARINT);
            int eTagPos = dst.position();
            VarIntsTrace.putVarInt(tagExp, dst, t);
            t.op("tag(expireAt)", "field", F_EXPIRE_AT, "wt", wtName(WT_VARINT),
                    "tag", hex(tagExp), "pos", eTagPos, "hex", hexRange(dst, eTagPos, dst.position()));
            int eStart = dst.position();
            long encT = ZigZagTrace.encode(vr.expireAtMillis(), t);
            VarIntsTrace.putVarLong(encT, dst, t);
            t.op("value(expireAt zigzag-varint)", "pos.from", eStart, "pos.to", dst.position(),
                    "hex", hexRange(dst, eStart, dst.position()));
        } else {
            t.why("expireAt=null → поле не пишем (optional)");
        }

        // --- value: field=3, wt=LEN (bytes) ---
        int tagVal = tag(F_VALUE, WT_LEN);
        int valTagPos = dst.position();
        VarIntsTrace.putVarInt(tagVal, dst, t);
        t.op("tag(value)", "field", F_VALUE, "wt", wtName(WT_LEN),
                "tag", hex(tagVal), "pos", valTagPos, "hex", hexRange(dst, valTagPos, dst.position()));
        int lenStart = dst.position();
        VarIntsTrace.putVarInt(vr.value().length, dst, t);
        int lenEnd = dst.position();
        t.op("length(value as varint)",
                "len", vr.value().length, "pos.from", lenStart, "pos.to", lenEnd,
                "hex", hexRange(dst, lenStart, lenEnd));
        int payloadFrom = dst.position();
        dst.put(vr.value());
        int payloadTo = dst.position();
        t.op("payload(value bytes)", "len", (payloadTo - payloadFrom),
                "pos.from", payloadFrom, "pos.to", payloadTo,
                "hex.head", hexHead(dst, payloadFrom, payloadTo, 32));

        // --- compressed: optional bool (varint=1) ---
        if (compressed) {
            int tagCmp = tag(F_COMPRESSED, WT_VARINT);
            int cTagPos = dst.position();
            VarIntsTrace.putVarInt(tagCmp, dst, t);
            t.op("tag(compressed)", "field", F_COMPRESSED, "wt", wtName(WT_VARINT),
                    "tag", hex(tagCmp), "pos", cTagPos, "hex", hexRange(dst, cTagPos, dst.position()));
            int cValPos = dst.position();
            VarIntsTrace.putVarInt(1, dst, t); // true
            t.op("value(compressed=true)", "pos", cValPos, "hex", hexRange(dst, cValPos, dst.position()));
        } else {
            t.why("compressed=false → поле не пишем (optional)");
        }

        t.out("PB.write", "dst_pos_after", dst.position(), "total.bytes", (dst.position() - start));
    }

    // ============================== READ ===============================

    public record Decoded(ValueRecord vr, boolean compressed) {
    }

    public static Decoded read(ByteBuffer src, Trace t) {
        int start = src.position();
        Long version = null, expire = null;
        boolean compressed = false;
        byte[] value = null;

        t.in("PB.read", "src_pos_before", start);

        while (src.hasRemaining()) {
            int tagPos = src.position();
            int tval = VarIntsTrace.getVarInt(src, t); // tag
            if (tval == 0) {
                t.why("встречен tag=0 → защитный выход");
                break;
            }
            int wt = tval & 0x07;
            int field = tval >>> 3;
            t.op("read tag", "pos", tagPos, "tag", hex(tval),
                    "field", field, "wt", wtName(wt), "hex", hexRange(src, tagPos, src.position()));

            switch (field) {
                case F_VERSION -> {
                    ensure(wt == WT_VARINT, "version: wire type mismatch", t);
                    int vPos = src.position();
                    long enc = VarIntsTrace.getVarLong(src, t);
                    long v = ZigZagTrace.decode(enc, t);
                    version = v;
                    t.op("version (zigzag-varint)", "pos.from", vPos, "pos.to", src.position(), "value", v);
                }
                case F_EXPIRE_AT -> {
                    ensure(wt == WT_VARINT, "expireAt: wire type mismatch", t);
                    int ePos = src.position();
                    long enc = VarIntsTrace.getVarLong(src, t);
                    long v = ZigZagTrace.decode(enc, t);
                    expire = v;
                    t.op("expireAt (zigzag-varint)", "pos.from", ePos, "pos.to", src.position(), "value", v);
                }
                case F_VALUE -> {
                    ensure(wt == WT_LEN, "value: wire type mismatch", t);
                    int lenPos = src.position();
                    int len = VarIntsTrace.getVarInt(src, t);
                    t.op("value.length", "len", len, "pos.from", lenPos, "pos.to", src.position());
                    if (len < 0 || len > 64 * 1024 * 1024) fail("value length out of bounds: " + len, t);
                    if (src.remaining() < len) fail("truncated value", t);
                    int pFrom = src.position();
                    value = new byte[len];
                    src.get(value);
                    int pTo = src.position();
                    t.op("value.bytes", "pos.from", pFrom, "pos.to", pTo,
                            "hex.head", hexHead(src, pFrom, pTo, 32));
                }
                case F_COMPRESSED -> {
                    ensure(wt == WT_VARINT, "compressed: wire type mismatch", t);
                    int cPos = src.position();
                    int iv = VarIntsTrace.getVarInt(src, t);
                    compressed = (iv != 0);
                    t.op("compressed flag", "raw", iv, "bool", compressed, "pos", cPos);
                }
                default -> {
                    t.why("неизвестное поле: пропустим по wire type");
                    skipUnknown(src, wt, t);
                }
            }
        }

        if (version == null || value == null) fail("missing required fields: version/value", t);
        ValueRecord vr = new ValueRecord(value, version, expire);
        t.out("PB.read", "src_pos_after", src.position(), "version", version, "expire", expire, "compressed", compressed, "value.len", value.length);
        return new Decoded(vr, compressed);
    }

    private static void skipUnknown(ByteBuffer src, int wt, Trace t) {
        switch (wt) {
            case WT_VARINT -> {
                int pos = src.position();
                long v = VarIntsTrace.getVarLong(src, t);
                t.op("skip VARINT", "pos.from", pos, "pos.to", src.position(), "hex", hexRange(src, pos, src.position()), "value", v);
            }
            case WT_LEN -> {
                int lenPos = src.position();
                int len = VarIntsTrace.getVarInt(src, t);
                if (len < 0 || src.remaining() < len) fail("truncated len-delimited", t);
                int from = src.position();
                src.position(src.position() + len);
                t.op("skip LEN", "len", len, "pos.from", lenPos, "payload.from", from, "payload.to", src.position(),
                        "hex.head", hexHead(src, from, src.position(), 32));
            }
            default -> fail("unsupported wire type: " + wt, t);
        }
    }

    // ============================== HELPERS =============================

    private static void ensure(boolean ok, String msg, Trace t) {
        if (!ok) fail(msg, t);
    }

    private static void fail(String msg, Trace t) {
        t.err("PB", msg);
        throw new CodecFormatException(msg);
    }

    public static final class CodecFormatException extends RuntimeException {
        public CodecFormatException(String m) {
            super(m);
        }
    }

    private static String hex(int v) {
        return String.format("0x%X", v);
    }

    private static String hexRange(ByteBuffer buf, int from, int to) {
        if (to <= from) return "∅";
        StringBuilder sb = new StringBuilder();
        for (int i = from; i < to; i++) sb.append(String.format("%02X ", buf.get(i)));
        return sb.toString().trim();
    }

    private static String hexHead(ByteBuffer buf, int from, int to, int limit) {
        int n = Math.max(0, Math.min(limit, to - from));
        String head = hexRange(buf, from, from + n);
        if (to - from > limit) return head + " …";
        return head;
    }
}
