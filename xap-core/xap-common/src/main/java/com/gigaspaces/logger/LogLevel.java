/*
 * Copyright (c) 2008-2016, GigaSpaces Technologies, Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
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
