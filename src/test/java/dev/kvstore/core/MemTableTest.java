package dev.kvstore.core;

import dev.kvstore.core.model.Entry;
import dev.kvstore.core.model.MemTable;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;


public class MemTableTest {

    private MemTable memTable;

    @BeforeEach
    void setUp() {
        this.memTable = new MemTable(10000);
    }

    @Test
    void getValue() {
        final var entry = new Entry("hello".getBytes(), "world".getBytes(), false);

        memTable.set(entry);
        final var value = new String(memTable.get("hello".getBytes()).value());

        assertEquals("world", value);
    }

    @Test
    void getAllEntries() {
        final var entry = new Entry("hello".getBytes(), "world".getBytes(), false);
        final var entry1 = new Entry("Arina".getBytes(), "Makunina".getBytes(), false);
        final var entry2 = new Entry("Ivan".getBytes(), "Morikov".getBytes(), false);

        memTable.set(entry);
        memTable.set(entry1);
        memTable.set(entry2);

        final var allEntries = memTable.getAllEntries();

        final List<Entry> entryList = allEntries.stream().map(Map.Entry::getValue).toList();

        assertTrue(entryList.contains(entry));
        assertTrue(entryList.contains(entry1));
        assertTrue(entryList.contains(entry2));
    }
}
