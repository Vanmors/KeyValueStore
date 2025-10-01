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
    private final int level;
    private final List<IndexEntry> index;
    private static final int BLOCK_SIZE = 4 * 1024; // 4KB на блок
    private static final int RESTART_INTERVAL = 16;

    public SSTable(String path, List<Entry> entries, int level) throws IOException {
        this.level = level;
        this.file = new File(path + ".sstable");
        this.index = new ArrayList<>();
        writeData(entries);
    }

    private void writeData(final List<Entry> entries) throws IOException {
        try (final FileOutputStream fos = new FileOutputStream(file)) {
            ByteBuffer blockBuf = ByteBuffer.allocate(BLOCK_SIZE);
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
        final ByteBuffer buf = ByteBuffer.allocate(entry.value().length + 1);
        buf.put(entry.value());
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

        // Читаем метаданные с конца последовательно
        int pos = buf.limit();
        pos -= 4; // Пропускаем crc32c
        buf.position(pos);
        int crc32c = buf.getInt();

        pos = readVarIntBackwards(buf, pos - 1, out -> {});
        long blockBaseExpire = readVarIntLong(buf, pos);

        pos = readVarIntBackwards(buf, pos - 1, out -> {});
        long blockBaseVersion = readVarIntLong(buf, pos);

        List<Integer> restartOffsets = new ArrayList<>();
        pos = readVarIntBackwards(buf, pos - 1, out -> {});
        int restartCount = VarInts.getVarInt(buf, pos);

        for (int i = 0; i < restartCount; i++) {
            pos = readVarIntBackwards(buf, pos - 1, offset -> restartOffsets.add(0, offset));
        }

        pos = readVarIntBackwards(buf, pos - 1, out -> {});
        int entries = VarInts.getVarInt(buf, pos);

        // Бинарный поиск по restart points
        int blockOffset = binarySearchRestartPoints(buf, restartOffsets, key);
        if (blockOffset < 0) {
            return null;
        }

        // Линейный поиск от точки перезапуска
        buf.position(blockOffset);
        byte[] lastKey = new byte[0];
        while (buf.position() < pos) { // До начала метаданных
            int shared = VarInts.getVarInt(buf);
            int unshared = VarInts.getVarInt(buf);
            int valueLen = VarInts.getVarInt(buf);

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
        if (restartOffsets.isEmpty()) {
            return -1;
        }

        int low = 0;
        int high = restartOffsets.size() - 1;

        while (low <= high) {
            int mid = (low + high) >>> 1;
            int offset = restartOffsets.get(mid);
            buf.position(offset);
            int shared = VarInts.getVarInt(buf);
            int unshared = VarInts.getVarInt(buf);
            int valueLen = VarInts.getVarInt(buf);
            byte[] keyBytes = new byte[shared + unshared];
            System.arraycopy(new byte[0], 0, keyBytes, 0, shared); // shared=0 для restart
            buf.get(keyBytes, shared, unshared);
            int cmp = Arrays.compare(key, keyBytes);

            if (cmp == 0) {
                return offset; // Точное совпадение
            }
            if (cmp < 0) {
                high = mid - 1; // Ключ меньше, ищем в левой половине
            } else {
                low = mid + 1; // Ключ больше, ищем в правой половине
            }
        }

        // Если ключ не найден, возвращаем ближайшую точку перезапуска, где ключ может быть
        int closest = high;
        if (closest < 0) {
            return restartOffsets.get(0); // Ключ меньше всех, начинаем с первой точки
        }
        if (closest >= restartOffsets.size()) {
            return restartOffsets.get(restartOffsets.size() - 1); // Ключ больше всех, начинаем с последней точки
        }
        return restartOffsets.get(closest);
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
