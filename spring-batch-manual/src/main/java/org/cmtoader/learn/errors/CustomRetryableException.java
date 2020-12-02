package org.cmtoader.learn.errors;

public class CustomRetryableException extends RuntimeException {

    public CustomRetryableException(String message) {
        super(message);
    }
}
