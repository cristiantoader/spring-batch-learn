package org.cmtoader.learn.errors;

import org.springframework.batch.item.ItemWriter;

import java.util.List;

public class SkipItemWriter implements ItemWriter<String> {

    private int retryCount;
    private boolean retry;

    public SkipItemWriter(boolean retry) {
        this.retry = retry;
    }

    @Override
    public void write(List<? extends String> items) {
        for (String item : items) {
            if (retry && item.equalsIgnoreCase("84")) {
                retryCount++;

                System.out.println("Writing of item failed " + item);
                throw new CustomRetryableException("Skipping item  " + item + " for " + retryCount + " times.");
            }

            System.out.println("Writing item " + item);
        }
    }
}
