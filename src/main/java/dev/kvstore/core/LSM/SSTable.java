package dev.kvstore.core.LSM;

import dev.kvstore.codec.BlockBuilder;
import dev.kvstore.codec.VarInts;
import dev.kvstore.core.model.Entry;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.function.Consumer;

public class SSTable {
    private final File file;
    private final List<IndexEntry> index;
    private static final int BLOCK_SIZE = 128;
    private static final int RESTART_INTERVAL = 16; // рестарт каждые 16 ключей

    public File file() {
        return file;
    }

    public long createdAtMillis() {
        return file.lastModified();
    }

    public SSTable(String path, List<Entry> entries) throws IOException {
        this.file = new File(path + ".sstable"); // для демо ок, но не очень красиво в будущем
        this.index = new ArrayList<>();
        writeData(entries);
    }

    private void writeData(final List<Entry> entries) throws IOException {
        try (final FileOutputStream fos = new FileOutputStream(file)) {
            ByteBuffer blockBuf = ByteBuffer.allocate(BLOCK_SIZE); // бьём файл на блоки
            BlockBuilder builder = new BlockBuilder(blockBuf, RESTART_INTERVAL);
            byte[] firstKey = null; // Теперь byte[]
            long blockOffset = 0;

            for (int i = 0; i < entries.size(); i++) {
                final Entry entry = entries.get(i);
                final ByteBuffer valueRecord = serializeValueRecord(entry);

                if (blockBuf.position() == 0) {
                    firstKey = entry.key(); // Сохраняем как byte[]
                }

                builder.add(entry.key(), valueRecord);

                if (blockBuf.position() > BLOCK_SIZE - 100 || i == entries.size() - 1) {
                    builder.finish();
                    blockBuf.flip();
                    fos.write(blockBuf.array(), 0, blockBuf.limit());
                    index.add(new IndexEntry(firstKey, blockOffset, blockBuf.limit()));
                    blockOffset += blockBuf.limit();
                    blockBuf = ByteBuffer.allocate(BLOCK_SIZE);
                    builder = new BlockBuilder(blockBuf, RESTART_INTERVAL);
                    firstKey = null;
                }
            }
            writeIndex(fos);
        }
    }

    private ByteBuffer serializeValueRecord(final Entry entry) {
        final int valLen = (entry.tombstone() || entry.value() == null) ? 0 : entry.value().length;
        final ByteBuffer buf = ByteBuffer.allocate(valLen + 1);
        if (valLen > 0) {
            buf.put(entry.value());
        }
        buf.put((byte) (entry.tombstone() ? 1 : 0));
        buf.flip();
        return buf;
    }

    private void writeIndex(FileOutputStream fos) throws IOException {
        final ByteBuffer indexBuf = ByteBuffer.allocate(estimateIndexSize());
        VarInts.putVarInt(index.size(), indexBuf);
        for (IndexEntry ie : index) {
            VarInts.putVarInt(ie.startKey.length, indexBuf);
            indexBuf.put(ie.startKey);
            VarInts.putVarLong(ie.offset, indexBuf);
            VarInts.putVarInt(ie.length, indexBuf);
        }
        indexBuf.flip();
        fos.write(indexBuf.array(), 0, indexBuf.limit());
    }

    private int estimateIndexSize() {
        return 4 + index.size() * 100; // Упрощенная оценка
    }

    public List<Entry> getAllEntries() throws IOException {
        List<Entry> out = new ArrayList<>();
        try (RandomAccessFile raf = new RandomAccessFile(file, "r")) {
            for (IndexEntry ie : index) {
                byte[] blockData = readBlock(ie.offset, ie.length);
                ByteBuffer buf = ByteBuffer.wrap(blockData);

                // ==== читаем трейлер РОВНО как у тебя сейчас ====
                int p = buf.limit();
                p -= 4; // crc
                buf.position(p);

                final int[] tmp = new int[1];
                p = readVarIntBackwards(buf, p - 1, v -> tmp[0] = v);
                long blockBaseExpire = tmp[0];

                p = readVarIntBackwards(buf, p - 1, v -> tmp[0] = v);
                long blockBaseVersion = tmp[0];

                List<Integer> restartOffsets = new ArrayList<>();
                p = readVarIntBackwards(buf, p - 1, v -> tmp[0] = v);
                int restartCount = tmp[0];

                for (int i = 0; i < restartCount; i++) {
                    final int[] off = new int[1];
                    p = readVarIntBackwards(buf, p - 1, v -> off[0] = v);
                    restartOffsets.add(0, off[0]); // прямой порядок
                }

                p = readVarIntBackwards(buf, p - 1, v -> tmp[0] = v);
                int entriesCount = tmp[0];
                // ==== конец чтения трейлера ====

                // Линейный проход с учётом рестартов
                buf.position(0);
                byte[] lastKey = new byte[0];
                int rIdx = 0;
                int nextRestart = (restartOffsets.isEmpty() ? Integer.MAX_VALUE : restartOffsets.get(0));

                while (buf.position() < p) {
                    // если попали на рестарт — сбрасываем lastKey
                    if (buf.position() == nextRestart) {
                        lastKey = new byte[0];
                        if (rIdx + 1 < restartOffsets.size()) {
                            nextRestart = restartOffsets.get(++rIdx);
                        } else {
                            nextRestart = Integer.MAX_VALUE;
                        }
                    }

                    int shared = VarInts.getVarInt(buf);
                    int unshared = VarInts.getVarInt(buf);
                    int valueLen = VarInts.getVarInt(buf);

                    // страховка от несогласованности: если shared > lastKey.length, считаем запись рестартом
                    if (shared > lastKey.length) {
                        shared = 0;
                    }

                    byte[] keyBytes = new byte[shared + unshared];
                    if (shared > 0) System.arraycopy(lastKey, 0, keyBytes, 0, shared);
                    buf.get(keyBytes, shared, unshared);

                    byte[] valueRecord = new byte[valueLen];
                    buf.get(valueRecord);

                    lastKey = keyBytes;

                    boolean tombstone = valueRecord[valueRecord.length - 1] == 1;
                    byte[] value = new byte[valueRecord.length - 1];
                    if (value.length > 0) System.arraycopy(valueRecord, 0, value, 0, value.length);

                    out.add(new Entry(keyBytes, value, tombstone));
                }
            }
        }
        return out;
    }

    public Entry search(final byte[] key) throws IOException {
        final IndexEntry block = searchIndex(key);
        if (block == null) {
            return null;
        }
        final byte[] blockData = readBlock(block.offset, block.length);
        return searchInBlock(blockData, key);
    }

    private IndexEntry searchIndex(final byte[] key) {
        int low = 0;
        int high = index.size() - 1;

        while (low <= high) {
            int mid = (low + high) >>> 1; // Беззнаковое деление
            IndexEntry midEntry = index.get(mid);
            int cmp = Arrays.compare(key, midEntry.startKey);
            if (cmp >= 0 && (mid == index.size() - 1 || Arrays.compare(key, index.get(mid + 1).startKey) < 0)) {
                return midEntry;
            }
            if (cmp < 0) {
                high = mid - 1;
            } else {
                low = mid + 1;
            }
        }
        return null;
    }

    private byte[] readBlock(final long offset, final int length) throws IOException {
        try (final RandomAccessFile raf = new RandomAccessFile(file, "r")) {
            raf.seek(offset);
            final byte[] blockData = new byte[length];
            raf.readFully(blockData);
            return blockData;
        }
    }

    private Entry searchInBlock(final byte[] blockData, final byte[] key) {
        ByteBuffer buf = ByteBuffer.wrap(blockData);

        // читаем трейлер блокa
        int p = buf.limit();
        p -= 4;
        buf.position(p);

        final int[] tmp = new int[1];

        p = readVarIntBackwards(buf, p - 1, v -> tmp[0] = v);
        long blockBaseExpire = tmp[0];

        p = readVarIntBackwards(buf, p - 1, v -> tmp[0] = v);
        long blockBaseVersion = tmp[0];

        List<Integer> restartOffsets = new ArrayList<>();
        p = readVarIntBackwards(buf, p - 1, v -> tmp[0] = v);
        int restartCount = tmp[0];

        for (int i = 0; i < restartCount; i++) {
            final int[] off = new int[1];
            p = readVarIntBackwards(buf, p - 1, v -> off[0] = v);
            restartOffsets.add(0, off[0]);
        }

        p = readVarIntBackwards(buf, p - 1, v -> tmp[0] = v);
        int entriesCount = tmp[0];

        // Бинарный поиск по restart points
        int blockOffset = binarySearchRestartPoints(buf, restartOffsets, key);
        if (blockOffset < 0) {
            return null;
        }

        // Линейный поиск от точки перезапуска
        buf.position(blockOffset);
        byte[] lastKey = new byte[0];
        while (buf.position() < p) { // До начала метаданных
            int shared = VarInts.getVarInt(buf);
            int unshared = VarInts.getVarInt(buf);
            int valueLen = VarInts.getVarInt(buf);

            if (shared > lastKey.length) {
                // повреждение или неверное смещение;
                return null;
            }

            byte[] keyBytes = new byte[shared + unshared];
            System.arraycopy(lastKey, 0, keyBytes, 0, shared);
            buf.get(keyBytes, shared, unshared);

            byte[] valueRecord = new byte[valueLen];
            buf.get(valueRecord);

            lastKey = keyBytes;

            if (Arrays.equals(keyBytes, key)) {
                boolean tombstone = valueRecord[valueRecord.length - 1] == 1;
                byte[] value = new byte[valueRecord.length - 1];
                System.arraycopy(valueRecord, 0, value, 0, value.length);
                return new Entry(keyBytes, value, tombstone);
            }
        }
        return null;
    }

    private int binarySearchRestartPoints(ByteBuffer buf, List<Integer> restartOffsets, byte[] key) {
        if (restartOffsets == null || restartOffsets.isEmpty()) {
            return 0; // нет рестартов — сканируем блок целиком
        }

        try {
            int low = 0, high = restartOffsets.size() - 1;
            while (low <= high) {
                int mid = (low + high) >>> 1;
                int offset = restartOffsets.get(mid);

                // читаем заголовок записи на рестарте
                buf.position(offset);
                int shared = VarInts.getVarInt(buf);
                int unshared = VarInts.getVarInt(buf);
                int valueLen = VarInts.getVarInt(buf);

                // На рестарте shared должен быть 0, но если нет — считаем метаданные битыми и падаем в линейный скан
                if (shared != 0 || unshared < 0) {
                    return 0;
                }

                // ключ на рестарте — это unshared байты
                byte[] keyBytes = new byte[unshared];
                buf.get(keyBytes);
                // скипаем value
                buf.position(buf.position() + valueLen);

                int cmp = Arrays.compare(key, keyBytes);
                if (cmp == 0) return offset;
                if (cmp < 0) high = mid - 1;
                else low = mid + 1;
            }

            int idx = Math.max(0, high);
            return restartOffsets.get(idx); // ближайший ≤ key
        } catch (RuntimeException e) {
            // любой сбой парсинга — безопасный фоллбэк
            return 0;
        }
    }

    // Вспомогательные методы для чтения varint назад
    private int readVarIntBackwards(ByteBuffer buf, int position, Consumer<Integer> consumer) {
        long result = 0;
        int shift = 0;
        int bytesRead = 0;

        while (bytesRead < 5) {
            byte b = buf.get(position - bytesRead);
            result |= (long) (b & 0x7F) << shift;
            bytesRead++;
            if (b >= 0) {
                consumer.accept((int) result);
                return position - bytesRead + 1;
            }
            shift += 7;
        }
        throw new IllegalArgumentException("Malformed varint at position " + position);
    }

    private long readVarIntLong(ByteBuffer buf, int position) {
        long result = 0;
        int shift = 0;
        int bytesRead = 0;

        while (bytesRead < 10) {
            byte b = buf.get(position - bytesRead);
            result |= (long) (b & 0x7F) << shift;
            bytesRead++;
            if (b >= 0) {
                return result;
            }
            shift += 7;
        }
        throw new IllegalArgumentException("Malformed varint at position " + position);
    }

    class IndexEntry {
        byte[] startKey;
        long offset;
        int length;

        IndexEntry(byte[] startKey, long offset, int length) {
            this.startKey = startKey;
            this.offset = offset;
            this.length = length;
        }
    }
}
