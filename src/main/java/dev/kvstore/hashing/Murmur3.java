package dev.kvstore.hashing;

import com.google.common.hash.HashFunction;
import com.google.common.hash.Hashing;
import java.nio.charset.StandardCharsets;

public class Murmur3 {
    private static final HashFunction HASH = Hashing.murmur3_128();

    public static long hash(final String key) {
        return HASH.hashString(key, StandardCharsets.UTF_8).asLong();
    }

    public static long hash(final byte[] key) {
        return HASH.hashBytes(key).asLong();
    }
}
