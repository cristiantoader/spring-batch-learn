package org.cmtoader.learn.errors;

import org.springframework.batch.core.SkipListener;

public class CustomSkipListener implements SkipListener<String, String> {

    @Override
    public void onSkipInRead(Throwable throwable) {

    }

    @Override
    public void onSkipInWrite(String s, Throwable throwable) {
        System.out.println("Skipping item writing " + s + " because of exception '" + throwable.getMessage() + "'");
    }

    @Override
    public void onSkipInProcess(String s, Throwable throwable) {
        System.out.println("Skipping item processing " + s + " because of exception '" + throwable.getMessage() + "'");
    }
}
