package com.suryadevathi.pipeline;

/** Malformed message. Retrying cannot help, so it goes straight to the dead-letter topic. */
public class InvalidEventException extends RuntimeException {
    public InvalidEventException(String message, Throwable cause) {
        super(message, cause);
    }

    public InvalidEventException(String message) {
        super(message);
    }
}
