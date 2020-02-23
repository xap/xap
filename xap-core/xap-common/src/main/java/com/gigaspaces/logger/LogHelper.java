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

import com.gigaspaces.CommonSystemProperties;

import java.text.MessageFormat;
import java.util.Date;
import java.util.logging.Level;
import java.util.logging.LogManager;
import java.util.logging.Logger;

/**
 * GS-8802 , GS-9973 , GS-10078 <br> A logging helper class used to avoid a deadlock situation when
 * java.util.logging is initializing, while it is also being referenced by utility classes. <br> The
 * {@link RollingFileHandler} resolves its PID, host, and homedir properties by calling utility
 * classes, which also try to reference a Logger using {@link Logger#getLogger(String)}. <br> This
 * causes a deadlock in JDK logging, since our custom handler is already called after holding a lock
 * on {@link LogManager} class and it is trying to also get a lock on {@link Logger} class when
 * calling {@link Logger#getLogger(String)} from utility classes. <br> When another thread calls
 * {@link Logger#getLogger(String)} it in turn calls LogManager.demandLogger(..) which locks {@link
 * LogManager} on a call to {@link LogManager#getLogger(String)}.
 *
 * <br> Deadlock: <br> lock [LogManager.class] -> LogManager.initializeGlobalHandlers() ->
 * RollingFileHandler.resolveProperty(..) -> wait to lock [Logger.class] on call to
 * Logger.getLogger(..) <br> lock [Logger.class] -> Logger.getLogger() -> LogManager.demandLogger()
 * -> wait to lock [LogManager.class] on call to LogManager.getLogger()
 *
 * <br>
 *
 * This helper class is intended to be called by utility classes which need to log messages. We go
 * through {@link LogManager#getLogger(String)} to avoid the deadlock. It may return
 * <code>null</code> if not yet initialized, in such a case we try to flush it to system.out/err.
 *
 * @author moran
 * @since 9.0.0
 */
@com.gigaspaces.api.InternalApi
public class LogHelper {

    public static final boolean ENABLED = !Boolean.getBoolean(CommonSystemProperties.GS_LOGGING_DISABLED);

    /**
     * log the message under the logName and level. if logger is <code>null</code> tries to log to
     * system.out/err
     *
     * @return <code>true</code> if logged to log or system.out/err, <code>false</code> otherwise.
     */
    public static boolean log(String logName, Level level, String message, Throwable thrown) {
        LogManager logManager = LogManager.getLogManager();
        Logger logger = logManager.getLogger(logName);
        if (logger != null && logger.isLoggable(level)) {
            logger.log(level, message, thrown);
            return true;
        }
        String key = logName + ".level";
        String levelOverride = System.getProperty(key, logManager.getProperty(key));
        if (levelOverride != null) {
            Level levelParsed = Level.parse(levelOverride);
            //isLoggable?
            if (level.intValue() < levelParsed.intValue() || level.equals(Level.OFF)) {
                return false;
            } else {
                println(logName, level, message, thrown);
            }
        }
        return false;
    }


    private static final MessageFormat messageFormat = new MessageFormat("{0,date,yyyy-MM-dd HH:mm:ss,SSS}");

    public static void println(String logName, Level level, String message) {
        println(logName, level, message, null);
    }

    public static void println(String logName, Level level, String message, Throwable t) {
        String dateTimeAsString = messageFormat.format(new Object[]{new Date()});
        String stackTrace = "";
        if (t != null) {
            stackTrace = "; Caught: " + t + "\n" + LogUtils.getStackTrace(t);
        }
        String msg = dateTimeAsString + " " + level + " [" + logName + "] - " + message + stackTrace;

        if (Level.WARNING.equals(level) || Level.SEVERE.equals(level)) {
            System.err.println(msg);
        } else {
            System.out.println(msg);
        }
    }

}
