package org.cmtoader.learn.reader;

import org.springframework.batch.item.*;

import java.util.List;

public class StatefulItemReader implements ItemStreamReader<String> {

    private final List<String> items;
    private int curIndex = -1;
    private boolean restart = false;

    public StatefulItemReader(List<String> items) {
        this.items = items;
        this.curIndex = 0;
    }

    @Override
    public String read() {
        if (this.curIndex <  this.items.size()) {
            return this.items.get(this.curIndex++);
        }

        return null;
    }

    @Override
    public void open(ExecutionContext executionContext) throws ItemStreamException {
        if (executionContext.containsKey("curIndex")) {
            this.curIndex = executionContext.getInt("curIndex");
            this.restart = true;

        } else {
            this.curIndex = 0;
            executionContext.put("curIndex", this.curIndex);
        }
    }

    @Override
    public void update(ExecutionContext executionContext) throws ItemStreamException {
        executionContext.put("curIndex", this.curIndex);
    }

    @Override
    public void close() throws ItemStreamException {

    }
}
