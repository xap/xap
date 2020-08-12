package com.gigaspaces.logger;


import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public interface LogLevel {

    boolean isEnabled(Logger logger);
    void log(Logger logger, String message);
    void log(Logger logger, String message, Object arg);
    void log(Logger logger, String message, Object ... args);

    LogLevel SEVERE = new LogLevel() {
        @Override
        public boolean isEnabled(Logger logger) {
            return logger.isErrorEnabled();
        }

        @Override
        public void log(Logger logger, String message) {
            logger.error(message);
        }

        @Override
        public void log(Logger logger, String message, Object arg) {
            logger.error(message, arg);
        }

        @Override
        public void log(Logger logger, String message, Object... args) {
            logger.error(message, args);
        }
    };

    LogLevel WARNING = new LogLevel() {
        @Override
        public boolean isEnabled(Logger logger) {
            return logger.isWarnEnabled();
        }

        @Override
        public void log(Logger logger, String message) {
            logger.warn(message);
        }

        @Override
        public void log(Logger logger, String message, Object arg) {
            logger.warn(message, arg);
        }

        @Override
        public void log(Logger logger, String message, Object... args) {
            logger.warn(message, args);
        }
    };

    LogLevel INFO = new LogLevel() {
        @Override
        public boolean isEnabled(Logger logger) {
            return logger.isInfoEnabled();
        }

        @Override
        public void log(Logger logger, String message) {
            logger.info(message);
        }

        @Override
        public void log(Logger logger, String message, Object arg) {
            logger.info(message, arg);
        }

        @Override
        public void log(Logger logger, String message, Object... args) {
            logger.info(message, args);
        }
    };

    LogLevel DEBUG = new LogLevel() {
        @Override
        public boolean isEnabled(Logger logger) {
            return logger.isDebugEnabled();
        }

        @Override
        public void log(Logger logger, String message) {
            logger.debug(message);
        }

        @Override
        public void log(Logger logger, String message, Object arg) {
            logger.debug(message, arg);
        }

        @Override
        public void log(Logger logger, String message, Object... args) {
            logger.debug(message, args);
        }
    };

    LogLevel TRACE = new LogLevel() {
        @Override
        public boolean isEnabled(Logger logger) {
            return logger.isTraceEnabled();
        }

        @Override
        public void log(Logger logger, String message) {
            logger.trace(message);
        }

        @Override
        public void log(Logger logger, String message, Object arg) {
            logger.trace(message, arg);
        }

        @Override
        public void log(Logger logger, String message, Object... args) {
            logger.trace(message, args);
        }
    };
}
