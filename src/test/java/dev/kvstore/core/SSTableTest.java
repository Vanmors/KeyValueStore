package dev.kvstore.core;

import dev.kvstore.core.LSM.SSTable;
import dev.kvstore.core.model.Entry;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;


public class SSTableTest {

    @Test
    void createSSTableWithOneEntry() throws IOException {
        final var entryList = List.of(new Entry("hello".getBytes(), "world".getBytes(), false));
        final var SSTable = new SSTable(".", entryList);

        final var result = SSTable.search("hello".getBytes());

        assertEquals("world", new String(result.value()));
    }

    @Test
    void createSSTableWithSomeEntries() throws IOException {
        final var entryList = List.of(new Entry("Ivan".getBytes(), "Morikov".getBytes(), false),
                new Entry("hello".getBytes(), "world".getBytes(), false));
        final var SSTable = new SSTable(".", entryList);

        final var result = SSTable.search("hello".getBytes());
        final var result1 = SSTable.search("Ivan".getBytes());

        assertEquals("world", new String(result.value()));
        assertEquals("Morikov", new String(result1.value()));
    }

    @Test
    void shouldWriteAndReadMultipleBlocks() throws Exception {
        final List<Entry> entries = new ArrayList<>();
        for (int i = 0; i < 200; i++) {
            final String key = "key" + i;
            final String value = "value" + i + "-".repeat(20);
            entries.add(new Entry(key.getBytes(), value.getBytes(), false));
        }

        final SSTable sstable = new SSTable(".", entries);

        final List<Entry> allEntries = sstable.getAllEntries();
        assertEquals(entries.size(), allEntries.size());

        final Entry found = sstable.search("key42".getBytes());
        assertNotNull(found);
        assertEquals("value42" + "-".repeat(20), new String(found.value()));

        final Entry notFound = sstable.search("unknown".getBytes());
        assertNull(notFound);
    }
}
