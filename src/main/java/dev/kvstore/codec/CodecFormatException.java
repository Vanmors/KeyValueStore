package dev.kvstore.codec;

public class CodecFormatException extends RuntimeException {
    public CodecFormatException(String msg) {
        super(msg);
    }

    public CodecFormatException(String msg, Throwable cause) {
        super(msg, cause);
    }
}
