package dev.kvstore.core;

import dev.kvstore.core.model.Entry;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.List;


public class SSTableTest {

    @Test
    void createSSTableWithOneEntry() throws IOException {
        final var entryList = List.of(new Entry("hello".getBytes(), "world".getBytes(), false));
        final var SSTable = new SSTable(".", entryList, 0);

        final var result = SSTable.search("hello".getBytes());

        System.out.println(new String(result.value()));
    }

    @Test
    void createSSTableWithSomeEntries() throws IOException {
        final var entryList = List.of(new Entry("hello".getBytes(), "world".getBytes(), false),
                new Entry("Ivan".getBytes(), "Morikov".getBytes(), false));
        final var SSTable = new SSTable(".", entryList, 0);

        final var result = SSTable.search("hello".getBytes());
        final var result1 = SSTable.search("Ivan".getBytes());

        System.out.println(new String(result.value()));
        System.out.println(new String(result1.value()));
    }
}
