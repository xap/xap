/*
 * 
 * Copyright 2005 Sun Microsystems, Inc.
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 * 	http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * 
 */
package com.gigaspaces.logger;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.lang.reflect.Method;
import java.util.logging.Level;
import java.util.logging.LogRecord;
import java.util.logging.Logger;

@com.gigaspaces.api.InternalApi
public class LogUtils {

    static String getStackTrace(Throwable t) {
        StringWriter sw = new StringWriter();
        PrintWriter pw = new PrintWriter(sw);
        t.printStackTrace(pw);
        pw.flush();

        return sw.toString();
    }

    public static long getCurrTimeIfNeeded(Logger logger, Level level) {
        return logger.isLoggable(level) ? System.currentTimeMillis() : 0;
    }

    public static void logDuration(Logger logger, Level level, long startTime, String message) {
        final long duration = System.currentTimeMillis() - startTime;
        logger.log(level, message + " [Duration = " + duration + "ms]");
    }

    public static void throwing(Logger logger, Class<?> sourceClass, String sourceMethod, Throwable thrown) {
        if (logger.isLoggable(Level.FINE)) {
            String message = sourceClass.getName() + "#" + sourceMethod;
            logger.log(Level.FINE, message, thrown);
        }
    }

    public static void throwing(Logger logger, Class<?> sourceClass, String sourceMethod, Throwable thrown,
                                String format, Object ... params) {
        if (logger.isLoggable(Level.FINE)) {
            String message = sourceClass.getName() + "#" + sourceMethod + ": " + format(format, params);
            logger.log(Level.FINE, message, thrown);
        }
    }

    private static String format(String format, Object ... params) {
        return params == null || params.length == 0 ? format : java.text.MessageFormat.format(format, params);
    }

    public static void entering(Logger logger, Class<?> sourceClass, String sourceMethod) {
        if (logger.isLoggable(Level.FINER))
            logger.entering(sourceClass.getName(), sourceMethod);
    }

    public static void entering(Logger logger, Class<?> sourceClass, String sourceMethod, Object arg) {
        if (logger.isLoggable(Level.FINER))
            logger.entering(sourceClass.getName(), sourceMethod, arg);
    }

    public static void entering(Logger logger, Class<?> sourceClass, String sourceMethod, Object[] args) {
        if (logger.isLoggable(Level.FINER))
            logger.entering(sourceClass.getName(), sourceMethod, args);
    }

    public static void entering(Logger logger, String sourceClass, String sourceMethod, Object[] args) {
        if (logger.isLoggable(Level.FINER))
            logger.entering(sourceClass, sourceMethod, args);
    }

    public static void exiting(Logger logger, Class<?> sourceClass, String sourceMethod) {
        if (logger.isLoggable(Level.FINER))
            logger.exiting(sourceClass.getName(), sourceMethod);
    }

    public static void exiting(Logger logger, Class<?> sourceClass, String sourceMethod, Object arg) {
        if (logger.isLoggable(Level.FINER))
            logger.exiting(sourceClass.getName(), sourceMethod, arg);
    }

    public static void exiting(Logger logger, String sourceClass, String sourceMethod, Object arg) {
        if (logger.isLoggable(Level.FINER))
            logger.exiting(sourceClass, sourceMethod, arg);
    }

    public static void exiting(Logger logger, Class<?> sourceClass, String sourceMethod, Object[] args) {
        if (logger.isLoggable(Level.FINER))
            logger.exiting(sourceClass.getName(), sourceMethod, args);
    }
}
