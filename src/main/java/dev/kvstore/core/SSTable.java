package dev.kvstore.core;

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
import java.util.Comparator;
import java.util.List;

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
            String firstKey = null;
            long blockOffset = 0;

            for (int i = 0; i < entries.size(); i++) {
                final Entry entry = entries.get(i);
                final ByteBuffer valueRecord = serializeValueRecord(entry);

                // Сохраняем первый ключ блока для индекса
                if (blockBuf.position() == 0) {
                    firstKey = new String(entry.key()); // Предполагаем UTF-8
                }

                // Добавляем в BlockBuilder
                builder.add(entry.key(), valueRecord);

                // Если блок полон или это последняя запись, завершаем блок
                if (blockBuf.position() > BLOCK_SIZE - 100 || i == entries.size() - 1) {
                    builder.finish(); // Записываем метаданные и CRC
                    blockBuf.flip();
                    fos.write(blockBuf.array(), 0, blockBuf.limit());

                    // Добавляем в индекс
                    index.add(new IndexEntry(firstKey, blockOffset, blockBuf.limit()));
                    blockOffset += blockBuf.limit();

                    // Новый блок
                    blockBuf = ByteBuffer.allocate(BLOCK_SIZE);
                    builder = new BlockBuilder(blockBuf, RESTART_INTERVAL);
                    firstKey = null;
                }
            }

            // Записываем индекс в конец файла
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
        // Сериализуем индекс: {startKey, offset, length}
        final ByteBuffer indexBuf = ByteBuffer.allocate(estimateIndexSize());
        VarInts.putVarInt(index.size(), indexBuf);
        for (IndexEntry ie : index) {
            final byte[] keyBytes = ie.startKey.getBytes();
            VarInts.putVarInt(keyBytes.length, indexBuf);
            indexBuf.put(keyBytes);
            VarInts.putVarLong(ie.offset, indexBuf);
            VarInts.putVarInt(ie.length, indexBuf);
        }
        indexBuf.flip();
        fos.write(indexBuf.array(), 0, indexBuf.limit());
    }

    private int estimateIndexSize() {
        return 4 + index.size() * 100; // хорошо бы нормально просчитать
    }

    public Entry search(final byte[] key) throws IOException {
        final IndexEntry block = searchIndex(Arrays.toString(key));
        if (block == null) return null;
        final byte[] blockData = readBlock(block.offset, block.length);
        return searchInBlock(blockData, key);
    }

    private IndexEntry searchIndex(final String key) {
        return index.stream()
                .filter(ie -> key.compareTo(ie.startKey) >= 0)
                .max(Comparator.comparing(a -> a.startKey))
                .orElse(null); // можно оптимизировать до нормального бинарного поиска
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
        // Читаем метаданные блока (в конце)
        int entries = VarInts.getVarInt(buf, buf.limit() - 4 - 8 - 8 - 4);
        int restartCount = VarInts.getVarInt(buf, buf.limit() - 4 - 8 - 8);
        List<Integer> restartOffsets = new ArrayList<>();
        for (int i = 0; i < restartCount; i++) {
            restartOffsets.add(VarInts.getVarInt(buf, buf.limit() - 4 - 8 - 8 + 4 * i));
        }

        // Бинарный поиск по restart points
        int blockOffset = binarySearchRestartPoints(buf, restartOffsets, Arrays.toString(key));
        if (blockOffset < 0) return null;

        // Линейный поиск от точки перезапуска
        buf.position(blockOffset);
        byte[] lastKey = new byte[0];
        while (buf.position() < buf.limit() - 4 - 8 - 8 - 4 * restartCount) {
            int shared = VarInts.getVarInt(buf);
            int unshared = VarInts.getVarInt(buf);
            int valueLen = VarInts.getVarInt(buf);

            byte[] keyBytes = new byte[shared + unshared];
            System.arraycopy(lastKey, 0, keyBytes, 0, shared);
            buf.get(keyBytes, shared, unshared);
            String currentKey = new String(keyBytes);

            byte[] valueRecord = new byte[valueLen];
            buf.get(valueRecord);

            lastKey = keyBytes;

            if (currentKey.equals(Arrays.toString(key))) {
                boolean tombstone = valueRecord[valueRecord.length - 1] == 1;
                byte[] value = new byte[valueRecord.length - 1];
                System.arraycopy(valueRecord, 0, value, 0, value.length);
                return new Entry(keyBytes, value, tombstone);
            }
        }
        return null;
    }

    private int binarySearchRestartPoints(ByteBuffer buf, List<Integer> restartOffsets, String key) {
        int low = 0, high = restartOffsets.size() - 1;
        while (low <= high) {
            int mid = (low + high) / 2;
            int offset = restartOffsets.get(mid);
            buf.position(offset);
            int shared = VarInts.getVarInt(buf);
            int unshared = VarInts.getVarInt(buf);
            int valueLen = VarInts.getVarInt(buf);
            byte[] keyBytes = new byte[shared + unshared];
            System.arraycopy(new byte[0], 0, keyBytes, 0, shared); // shared=0 для restart
            buf.get(keyBytes, shared, unshared);
            String restartKey = new String(keyBytes);
            int cmp = key.compareTo(restartKey);
            if (cmp == 0) return offset;
            if (cmp < 0) high = mid - 1;
            else low = mid + 1;
        }
        return restartOffsets.get(high < 0 ? 0 : high);
    }


    class IndexEntry {
        String startKey;
        long offset;
        int length;

        IndexEntry(String startKey, long offset, int length) {
            this.startKey = startKey;
            this.offset = offset;
            this.length = length;
        }
    }
}
