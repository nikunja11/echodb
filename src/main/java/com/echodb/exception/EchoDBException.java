package com.echodb.exception;

/**
 * Custom exception for SlateDB operations
 */
public class EchoDBException extends Exception {
    
    public EchoDBException(String message) {
        super(message);
    }
    
    public EchoDBException(String message, Throwable cause) {
        super(message, cause);
    }
    
    public EchoDBException(Throwable cause) {
        super(cause);
    }
}