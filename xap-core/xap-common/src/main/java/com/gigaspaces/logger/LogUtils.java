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
import java.util.StringJoiner;
import java.util.logging.Level;
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

    public static long getCurrTimeIfDebugEnabled(Logger logger) {
        return logger.isLoggable(Level.FINE) ? System.currentTimeMillis() : 0;
    }

    public static long getCurrTimeIfTraceEnabled(Logger logger) {
        return logger.isLoggable(Level.FINEST) ? System.currentTimeMillis() : 0;
    }


    public static String formatDuration(long startTime, String message) {
        final long duration = System.currentTimeMillis() - startTime;
        return message + " [Duration = " + duration + "ms]";
    }

    public static String format(Class<?> sourceClass, String sourceMethod, String format, Object ... params) {
        if (format == null || format.isEmpty())
            return sourceClass.getName() + "#" + sourceMethod;
        if (params == null || params.length == 0)
            return sourceClass.getName() + "#" + sourceMethod + ": " + format;
        else
            return sourceClass.getName() + "#" + sourceMethod + ": " + java.text.MessageFormat.format(format, params);
    }

    public static void throwing(Logger logger, Class<?> sourceClass, String sourceMethod, Throwable thrown) {
        if (logger.isLoggable(Level.FINE)) {
            logger.log(Level.FINE, action("throw", sourceClass.getName(), sourceMethod), thrown);
        }
    }

    public static void throwing(Logger logger, Class<?> sourceClass, String sourceMethod, Throwable thrown,
                                String format, Object ... params) {
        if (logger.isLoggable(Level.FINE)) {
            logger.log(Level.FINE, format(sourceClass, sourceMethod, format, params), thrown);
        }
    }

    public static void entering(Logger logger, Class<?> sourceClass, String sourceMethod) {
        if (logger.isLoggable(Level.FINER))
            logger.log(Level.FINER, action("enter", sourceClass.getName(), sourceMethod));
    }

    public static void entering(Logger logger, Class<?> sourceClass, String sourceMethod, Object arg) {
        if (logger.isLoggable(Level.FINER))
            logger.log(Level.FINER, action("enter", sourceClass.getName(), sourceMethod, arg));
    }

    public static void entering(Logger logger, Class<?> sourceClass, String sourceMethod, Object[] args) {
        if (logger.isLoggable(Level.FINER))
            logger.log(Level.FINER, action("enter", sourceClass.getName(), sourceMethod, args));
    }

    public static void entering(Logger logger, String sourceClass, String sourceMethod, Object[] args) {
        if (logger.isLoggable(Level.FINER))
            logger.log(Level.FINER, action("enter", sourceClass, sourceMethod, args));
    }

    public static void exiting(Logger logger, Class<?> sourceClass, String sourceMethod) {
        if (logger.isLoggable(Level.FINER))
            logger.log(Level.FINER, action("exit", sourceClass.getName(), sourceMethod));
    }

    public static void exiting(Logger logger, Class<?> sourceClass, String sourceMethod, Object arg) {
        if (logger.isLoggable(Level.FINER))
            logger.log(Level.FINER, action("exit", sourceClass.getName(), sourceMethod, arg));
    }

    public static void exiting(Logger logger, String sourceClass, String sourceMethod, Object arg) {
        if (logger.isLoggable(Level.FINER))
            logger.log(Level.FINER, action("exit", sourceClass, sourceMethod, arg));
    }

    public static void exiting(Logger logger, Class<?> sourceClass, String sourceMethod, Object[] args) {
        if (logger.isLoggable(Level.FINER))
            logger.log(Level.FINER, action("exit", sourceClass.getName(), sourceMethod, args));
    }

    private static String action(String action, String sourceClass, String sourceMethod, Object ... params) {
        if (params == null || params.length == 0)
            return sourceClass + "#" + sourceMethod + ": " + action;
        if (params.length == 1)
            return sourceClass + "#" + sourceMethod + ": " + action + " with (" + params[0] + ")";
        StringJoiner sj = new StringJoiner(", ", sourceClass + "#" + sourceMethod + ": " + action + " with (", ")");
        for (Object param : params) {
            sj.add(String.valueOf(param));
        }
        return sj.toString();
    }
}
