package org.example.streamLoad.exception;

public class DorisRuntimeException extends RuntimeException {
    public DorisRuntimeException() {
        super();
    }

    public DorisRuntimeException(String message) {
        super(message);
    }

    public DorisRuntimeException(String message, Throwable cause) {
        super(message, cause);
    }

    public DorisRuntimeException(Throwable cause) {
        super(cause);
    }

    protected DorisRuntimeException(String message, Throwable cause,
                                    boolean enableSuppression,
                                    boolean writableStackTrace) {
        super(message, cause, enableSuppression, writableStackTrace);
    }
}
