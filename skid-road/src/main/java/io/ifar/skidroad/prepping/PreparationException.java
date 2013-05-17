package io.ifar.skidroad.prepping;

/**
 * Thrown by PrepWorker implementations.
*/
public class PreparationException extends Exception {
    public PreparationException() {
    }

    public PreparationException(Throwable cause) {
        super(cause);
    }

    public PreparationException(String message) {
        super(message);
    }

    public PreparationException(String message, Throwable cause) {
        super(message, cause);
    }

    public PreparationException(String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
        super(message, cause, enableSuppression, writableStackTrace);
    }
}
