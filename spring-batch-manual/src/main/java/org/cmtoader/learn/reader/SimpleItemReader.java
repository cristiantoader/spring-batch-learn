package org.cmtoader.learn.reader;

import org.springframework.batch.item.ItemReader;

import java.util.Iterator;
import java.util.Optional;

public class SimpleItemReader implements ItemReader<String> {

    private final Iterator<String> items;

    public SimpleItemReader(Iterator<String> items) {
        this.items = items;
    }

    @Override
    public String read() {
        return Optional.of(items)
                       .filter(Iterator::hasNext)
                       .map(Iterator::next)
                       .orElse(null);
    }
}
