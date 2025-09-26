package dev.kvstore.codec;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.zip.CRC32C;

/**
 * <p>Разбор идеи: <a href="https://medium.com/%40xuezaigds/leveldb-explained-prefix-compression-and-restart-points-in-blockbuilder-5ebeb51c2b0d">
 * LevelDB explained: prefix compression & restart points</a>
 *
 * <p>Упаковка отсортированной последовательности пар
 * {@code (key, ValueRecord-bytes)} в байтовый буфер для последующего
 * бинарного поиска внутри блока и сканирования.
 * <p>
 * Для каждой записи ключ кодируется как:
 * <ul>
 *   <li>{@code sharedPrefixLen} — varint длина общего префикса с предыдущим ключом,</li>
 *   <li>{@code unsharedKeyLen} — varint длина «хвоста» ключа,</li>
 *   <li>{@code valueRecordLen} — varint длина сериализованного {@code ValueRecord},</li>
 *   <li>{@code unsharedKeyBytes[unsharedKeyLen]},</li>
 *   <li>{@code valueRecordBytes[valueRecordLen]}.</li>
 * </ul>
 * В конец блока последовательно дописываются:
 * <ul>
 *   <li>{@code entryCount} — varint количество записей,</li>
 *   <li>{@code restartCount} — varint число точек перезапуска</li>
 *   <li>{@code restartOffsets[restartCount]} — varint-смещения от начала блока</li>
 *   <li>{@code blockBaseVersion} — varint базовая версия для дельт версий</li>
 *   <li>{@code blockBaseExpire} — varint базовый expireAt для дельт TTL/li>
 *   <li>{@code crc32c} — 4-байтовая CRC32C на всё содержимое блока</li>
 * </ul>
 * </ul>
 */
public final class BlockBuilder {
    /**
     * Через сколько записей писать полный ключ
     */
    private final int restartInterval;
    /**
     * Целевой буфер
     */
    private final ByteBuffer buf;
    /**
     * Смещения точек перезапуска от начала блока при поиске
     */
    private final List<Integer> restartOffsets = new ArrayList<>();
    /**
     * Последний добавленный ключ для вычисления общего префикса
     */
    private byte[] lastKey = new byte[0];
    /**
     * Количество добавленных записей
     */
    private int entries;
    /**
     * Версия блока
     */
    public long blockBaseVersion = 0;
    /**
     * TTL
     */
    public Long blockBaseExpire = null;

    /**
     * @param buf             целевой буфер вывода; позиция буфера будет продвигаться по мере записи
     * @param restartInterval интервал рестартов префиксного сжатия
     */
    public BlockBuilder(ByteBuffer buf, int restartInterval) {
        this.buf = buf;
        this.restartInterval = restartInterval;
    }

    /**
     * Добавить запись {@code (key, valueRecordSlice)} в блок.
     *
     * @param key              полный ключ (byte[]) текущей записи; ожидается сортировка по возрастанию
     * @param valueRecordSlice срез буфера, содержащий байтовое представление {@code ValueRecord}
     */
    public void add(byte[] key, ByteBuffer valueRecordSlice) {
        int shared = sharedPrefixLen(lastKey, key);
        boolean isRestart = (entries % restartInterval) == 0;
        if (isRestart) {
            shared = 0;
            // позиция начала этой записи — смещение для индекса рестартов
            restartOffsets.add(buf.position());
        }
        int unshared = key.length - shared;

        // префиксное сжатие ключа + длина ValueRecord
        VarInts.putVarInt(shared, buf);
        VarInts.putVarInt(unshared, buf);
        VarInts.putVarInt(valueRecordSlice.remaining(), buf);

        // хвост ключа
        buf.put(key, shared, unshared);
        buf.put(valueRecordSlice.duplicate());

        lastKey = key;
        entries++;
    }

    /**
     * Завершение формирования блока.
     */
    public void finish() {
        // счётчики и рестарты
        VarInts.putVarInt(entries, buf);
        VarInts.putVarInt(restartOffsets.size(), buf);
        for (int off : restartOffsets) VarInts.putVarInt(off, buf);

        // базовые значения блока для дельт кодеков
        VarInts.putVarLong(blockBaseVersion, buf);
        VarInts.putVarLong(blockBaseExpire == null ? 0 : blockBaseExpire, buf);

        CRC32C crc = new CRC32C();

        // считаем CRC по уже записанным байтам
        ByteBuffer view = buf.duplicate();
        view.flip();
        crc.update(view);

        // CRC в конец
        buf.putInt((int) crc.getValue());
    }

    /**
     * Длина общего префикса двух массивов байт.
     */
    private static int sharedPrefixLen(byte[] a, byte[] b) {
        int i = 0, n = Math.min(a.length, b.length);
        while (i < n && a[i] == b[i]) i++;
        return i;
    }
}
