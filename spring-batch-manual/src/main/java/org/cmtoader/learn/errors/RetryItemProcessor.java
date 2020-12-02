package org.cmtoader.learn.errors;

import org.springframework.batch.item.ItemProcessor;

public class RetryItemProcessor implements ItemProcessor<String, String> {

    private boolean retry = false;
    private int attemptCount = 1;

    public RetryItemProcessor(boolean retry) {
        this.retry = retry;
    }

    @Override
    public String process(String item) {
        System.out.println("Processing item " + item);

        if (retry && item.equalsIgnoreCase("42")) {
            attemptCount++;

            if(attemptCount >= 5) {
                System.out.println("Success!");
                retry = false;
                return String.valueOf(Integer.valueOf(item) * -1);

            } else {
                System.out.println("Processing of item " + item + " has failed.");
                throw new CustomRetryableException("Processing faield - attempt " + attemptCount);
            }

        } else {
            return String.valueOf(Integer.valueOf(item) * -1);
        }
    }
}
