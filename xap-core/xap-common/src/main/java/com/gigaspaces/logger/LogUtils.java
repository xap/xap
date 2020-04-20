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

import org.slf4j.Logger;
import org.slf4j.helpers.MessageFormatter;

@com.gigaspaces.api.InternalApi
public class LogUtils {

    private void foo() {
        LogLevel.INFO.isEnabled(null);
    }
    static String getStackTrace(Throwable t) {
        StringWriter sw = new StringWriter();
        PrintWriter pw = new PrintWriter(sw);
        t.printStackTrace(pw);
        pw.flush();

        return sw.toString();
    }

    public static long getCurrTimeIfDebugEnabled(Logger logger) {
        return logger.isDebugEnabled() ? System.currentTimeMillis() : 0;
    }

    public static long getCurrTimeIfTraceEnabled(Logger logger) {
        return logger.isTraceEnabled() ? System.currentTimeMillis() : 0;
    }


    public static String formatDuration(long startTime, String message) {
        final long duration = System.currentTimeMillis() - startTime;
        return message + " [Duration = " + duration + "ms]";
    }

    public static String format(String message, Object ... args) {
        if (args == null || args.length == 0)
            return message;
        return MessageFormatter.arrayFormat(message, args, null).getMessage();
    }

    public static String format(Class<?> sourceClass, String sourceMethod, String format, Object ... params) {
        if (format == null || format.isEmpty())
            return sourceClass.getName() + "#" + sourceMethod;
        if (params == null || params.length == 0)
            return sourceClass.getName() + "#" + sourceMethod + ": " + format;
        else
            return sourceClass.getName() + "#" + sourceMethod + ": " + format(format, params);
    }

    public static void throwing(Logger logger, Class<?> sourceClass, String sourceMethod, Throwable thrown) {
        if (logger.isDebugEnabled()) {
            logger.debug(action("throw", sourceClass.getName(), sourceMethod), thrown);
        }
    }

    public static void throwing(Logger logger, Class<?> sourceClass, String sourceMethod, Throwable thrown,
                                String format, Object ... params) {
        if (logger.isDebugEnabled()) {
            logger.debug(format(sourceClass, sourceMethod, format, params), thrown);
        }
    }

    public static void throwing(LogLevel level, Logger logger, Class<?> sourceClass, String sourceMethod, Throwable thrown,
                                String format, Object ... params) {
        if (level.isEnabled(logger)) {
            level.log(logger, format(sourceClass, sourceMethod, format, params), thrown);
        }
    }

    public static void entering(Logger logger, Class<?> sourceClass, String sourceMethod) {
        if (logger.isDebugEnabled())
            logger.debug(action("enter", sourceClass.getName(), sourceMethod));
    }

    public static void entering(Logger logger, Class<?> sourceClass, String sourceMethod, Object arg) {
        if (logger.isDebugEnabled())
            logger.debug(action("enter", sourceClass.getName(), sourceMethod, arg));
    }

    public static void entering(Logger logger, Class<?> sourceClass, String sourceMethod, Object[] args) {
        if (logger.isDebugEnabled())
            logger.debug(action("enter", sourceClass.getName(), sourceMethod, args));
    }

    public static void entering(Logger logger, String sourceClass, String sourceMethod, Object[] args) {
        if (logger.isDebugEnabled())
            logger.debug(action("enter", sourceClass, sourceMethod, args));
    }

    public static void exiting(Logger logger, Class<?> sourceClass, String sourceMethod) {
        if (logger.isDebugEnabled())
            logger.debug(action("exit", sourceClass.getName(), sourceMethod));
    }

    public static void exiting(Logger logger, Class<?> sourceClass, String sourceMethod, Object arg) {
        if (logger.isDebugEnabled())
            logger.debug(action("exit", sourceClass.getName(), sourceMethod, arg));
    }

    public static void exiting(Logger logger, String sourceClass, String sourceMethod, Object arg) {
        if (logger.isDebugEnabled())
            logger.debug(action("exit", sourceClass, sourceMethod, arg));
    }

    public static void exiting(Logger logger, Class<?> sourceClass, String sourceMethod, Object[] args) {
        if (logger.isDebugEnabled())
            logger.debug(action("exit", sourceClass.getName(), sourceMethod, args));
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
