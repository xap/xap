package com.gigaspaces.logger;

import java.util.logging.Level;
import java.util.logging.Logger;

public interface LogLevel {

    boolean isEnabled(Logger logger);
    void log(Logger logger, String message);
    void log(Logger logger, String message, Object arg);
    void log(Logger logger, String message, Object ... args);

    LogLevel SEVERE = new LogLevel() {
        @Override
        public boolean isEnabled(Logger logger) {
            return logger.isLoggable(Level.SEVERE);
        }

        @Override
        public void log(Logger logger, String message) {
            logger.log(Level.SEVERE, message);
        }

        @Override
        public void log(Logger logger, String message, Object arg) {
            logger.log(Level.SEVERE, message, arg);
        }

        @Override
        public void log(Logger logger, String message, Object... args) {
            logger.log(Level.SEVERE, message, args);
        }
    };

    LogLevel WARNING = new LogLevel() {
        @Override
        public boolean isEnabled(Logger logger) {
            return logger.isLoggable(Level.WARNING);
        }

        @Override
        public void log(Logger logger, String message) {
            logger.log(Level.WARNING, message);
        }

        @Override
        public void log(Logger logger, String message, Object arg) {
            logger.log(Level.WARNING, message, arg);
        }

        @Override
        public void log(Logger logger, String message, Object... args) {
            logger.log(Level.WARNING, message, args);
        }
    };

    LogLevel INFO = new LogLevel() {
        @Override
        public boolean isEnabled(Logger logger) {
            return logger.isLoggable(Level.INFO);
        }

        @Override
        public void log(Logger logger, String message) {
            logger.log(Level.INFO, message);
        }

        @Override
        public void log(Logger logger, String message, Object arg) {
            logger.log(Level.INFO, message, arg);
        }

        @Override
        public void log(Logger logger, String message, Object... args) {
            logger.log(Level.INFO, message, args);
        }
    };

    LogLevel DEBUG = new LogLevel() {
        @Override
        public boolean isEnabled(Logger logger) {
            return logger.isLoggable(Level.FINE);
        }

        @Override
        public void log(Logger logger, String message) {
            logger.log(Level.FINE, message);
        }

        @Override
        public void log(Logger logger, String message, Object arg) {
            logger.log(Level.FINE, message, arg);
        }

        @Override
        public void log(Logger logger, String message, Object... args) {
            logger.log(Level.FINE, message, args);
        }
    };

    LogLevel TRACE = new LogLevel() {
        @Override
        public boolean isEnabled(Logger logger) {
            return logger.isLoggable(Level.FINEST);
        }

        @Override
        public void log(Logger logger, String message) {
            logger.log(Level.FINEST, message);
        }

        @Override
        public void log(Logger logger, String message, Object arg) {
            logger.log(Level.FINEST, message, arg);
        }

        @Override
        public void log(Logger logger, String message, Object... args) {
            logger.log(Level.FINEST, message, args);
        }
    };
}
