package dev.kvstore.core;

import dev.kvstore.core.model.Entry;
import dev.kvstore.core.model.WALEntry;
import dev.kvstore.core.model.WALOperationType;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;


public class WALImplTest {

    private WAL wal;

    @BeforeEach
    void setUp() throws IOException {
        this.wal = new WALImpl("wal.log");
        wal.clear();
    }

    @Test
    void writeAndReadWal() throws IOException {
        final Entry entry = new Entry("hello".getBytes(), "world".getBytes(), false);
        final Entry entry1 = new Entry("foo".getBytes(), "bar".getBytes(), false);

        wal.write(entry, WALOperationType.PUT);
        wal.write(entry1, WALOperationType.DELETE);

        final List<WALEntry> recovered = new ArrayList<>();
        wal.recover(recovered::add);

        assertEquals(2, recovered.size());
        assertEquals(new String(recovered.get(0).value()), "world");
        assertEquals(new String(recovered.get(1).value()), "bar");
    }
}
