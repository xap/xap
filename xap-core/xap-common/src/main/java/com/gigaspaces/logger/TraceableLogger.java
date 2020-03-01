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

import java.lang.ref.WeakReference;
import java.util.Arrays;
import java.util.Comparator;
import java.util.Iterator;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.logging.Level;
import java.util.logging.LogRecord;
import java.util.logging.Logger;

@com.gigaspaces.api.InternalApi
public class TraceableLogger {
    private static final int DEFAULT_TRACE_LENGTH = 100;
    private static final int OFF_VALUE = Level.OFF.intValue();
    private static final Comparator<LogRecord> comperator = new Comparator<LogRecord>() {

        public int compare(LogRecord o1, LogRecord o2) {
            if (o1 == o2) return 0;
            if (o1 == null) return 1;
            if (o2 == null) return -1;

            if (o1.getMillis() == o2.getMillis())
                return ((Long) o1.getSequenceNumber()).compareTo(o2.getSequenceNumber());
            return ((Long) o1.getMillis()).compareTo(o2.getMillis());
        }
    };

    private static final ConcurrentMap<String, WeakReference<TraceableLogger>> traceableLoggers = new ConcurrentHashMap<String, WeakReference<TraceableLogger>>();

    private final int _traceLoggingLevel;
    private final int _traceLength;

    private final CustomLogger _customLogger;
    private final Logger _logger;
    private final String[] _associatedLoggers;
    private final boolean _hasAssociatedLogOn;

    private final ThreadLocal<LogRecord[]> _threadTrace;
    private final LogRecord[] _globalTrace;
    private final boolean _threadLocalTraceEnabled;

    private final AtomicInteger _globalTraceIndex = new AtomicInteger();
    private final ThreadLocal<Integer> _threadLocalTraceIndex = new ThreadLocal<Integer>() {
        protected Integer initialValue() {
            return 0;
        }
    };

    private final ReadWriteLock _rwLock = new ReentrantReadWriteLock(false);


    //Not meant to be instantiated from outside
    private TraceableLogger(String name, Logger logger, String... associatedLoggers) {
        _customLogger = new CustomLogger(name);
        _logger = logger;
        _associatedLoggers = associatedLoggers;
        //Get trace logging level
        Level traceLevel = getDefaultTraceLevel(name);
        traceLevel = Level.parse(System.getProperty(name + ".level", traceLevel.getName()));
        if (traceLevel.intValue() != OFF_VALUE)
            _customLogger.setLevel(traceLevel);

        _traceLoggingLevel = traceLevel.intValue();
        //Get trace length
        _traceLength = Integer.getInteger(name + ".length", getDefaultTraceLength(name));

        boolean threadLocalTraceEnabled = getDefaultThreadLocalTraceEnabled(name);
        threadLocalTraceEnabled |= Boolean.getBoolean(name + ".thread");

        if (threadLocalTraceEnabled) {
            _threadLocalTraceEnabled = true;
            _threadTrace = new ThreadLocal<LogRecord[]>() {
                @Override
                protected LogRecord[] initialValue() {
                    return new LogRecord[_traceLength];
                }
            };
        } else {
            _threadLocalTraceEnabled = false;
            _threadTrace = null;
        }

        _globalTrace = new LogRecord[_traceLength];

        boolean tempHasAssociatedLogOn = false;
        if (_associatedLoggers != null) {
            for (String associatedLogName : _associatedLoggers) {
                if (TraceableLogger.getLogger(associatedLogName).getTraceLevel() != OFF_VALUE) {
                    tempHasAssociatedLogOn = true;
                    break;
                }
            }
        }
        _hasAssociatedLogOn = tempHasAssociatedLogOn;
    }

    /**
     * Find or create a logger for a named subsystem.  If a logger has already been created with the
     * given name it is returned.  Otherwise a new logger is created.
     *
     * @see Logger#getLogger(String)
     */
    public static TraceableLogger getLogger(String name) {
        synchronized (Logger.class) {
            removeUnusedWeakReferences();

            final String traceableLoggerName = name + ".trace";

            WeakReference<TraceableLogger> traceableLoggerWeakReference = traceableLoggers.get(traceableLoggerName);
            if (traceableLoggerWeakReference != null) {
                TraceableLogger traceableLogger = traceableLoggerWeakReference.get();
                if (traceableLogger != null)
                    return traceableLogger;
            }

            Logger logger = Logger.getLogger(name);
            TraceableLogger traceableLogger = new TraceableLogger(traceableLoggerName, logger, getAssociatedLoggers(name));
            traceableLoggers.put(traceableLoggerName, new WeakReference<TraceableLogger>(traceableLogger));

            return traceableLogger;
        }
    }

    /**
     * Removes irrelevant weak references from the static traceable loggers map. Should be called
     * under a lock.
     */
    private static void removeUnusedWeakReferences() {
        Iterator<Entry<String, WeakReference<TraceableLogger>>> iterator = traceableLoggers.entrySet().iterator();
        while (iterator.hasNext()) {
            if (iterator.next().getValue().get() == null)
                iterator.remove();
        }
    }

    public int getTraceLevel() {
        return _traceLoggingLevel;
    }

    /**
     * Display the trace of the current thread
     */
    public synchronized void showThreadTrace() {
        if (!_threadLocalTraceEnabled)
            throw new IllegalStateException();

        if (_traceLoggingLevel == OFF_VALUE && !_hasAssociatedLogOn)
            return;
        _logger.log(Level.INFO, "Trace Thread Start [" + _traceLength + "]");
        sortTrace(_threadTrace.get());
        for (LogRecord record : _threadTrace.get())
            traceLogRecord(record);
        if (_associatedLoggers != null) {
            for (String associatedLogName : _associatedLoggers)
                TraceableLogger.getLogger(associatedLogName).showThreadTrace();
        }
        _logger.log(Level.INFO, "Trace Thread End [" + _traceLength + "]");
    }

    public synchronized void clearThreadTrace() {
        if (!_threadLocalTraceEnabled)
            throw new IllegalStateException();
        _threadTrace.remove();
    }

    /**
     * Display the global trace
     */
    public synchronized void showGlobalTrace() {
        acquireExclusiveAccess();
        try {
            if (_traceLoggingLevel == OFF_VALUE && !_hasAssociatedLogOn)
                return;
            _logger.log(Level.INFO, "Trace Global Start [" + _traceLength + "]");
            sortTrace(_globalTrace);
            for (LogRecord record : _globalTrace)
                traceLogRecord(record);
            if (_associatedLoggers != null) {
                for (String associatedLogName : _associatedLoggers)
                    TraceableLogger.getLogger(associatedLogName).showGlobalTrace();
            }
            _logger.log(Level.INFO, "Trace Global End [" + _traceLength + "]");
        } finally {
            releaseExclusiveAccess();
        }
    }

    private static void sortTrace(LogRecord[] logRecords) {
        Arrays.sort(logRecords, comperator);
    }

    private void releaseAccess() {
        _rwLock.readLock().unlock();
    }

    private void acquireAccess() {
        _rwLock.readLock().lock();
    }


    private void releaseExclusiveAccess() {
        _rwLock.writeLock().unlock();
    }

    private void acquireExclusiveAccess() {
        _rwLock.writeLock().lock();
    }

    private void traceLogRecord(LogRecord record) {
        if (record == null)
            return;
        record.setLevel(Level.INFO);
        if (!record.getMessage().startsWith("[tid=")) {
            record.setMessage("[tid=" + record.getThreadID() + "] " + record.getMessage());
        }
        _logger.log(record);
    }

    private static String[] getAssociatedLoggers(String name) {
        if (name.equals(Constants.LOGGER_LRMI))
            return new String[]{Constants.LOGGER_LRMI_MARSHAL};
        return null;
    }

    private static boolean getDefaultThreadLocalTraceEnabled(String name) {
        if (name.equals(Constants.LOGGER_LRMI_CLASSLOADING + ".trace"))
            return false;

        return true;
    }

    private static Level getDefaultTraceLevel(String name) {
        if (name.equals(Constants.LOGGER_LRMI_CLASSLOADING + ".trace"))
            return Level.FINEST;
        if (name.equals(Constants.LOGGER_REPLICATION_BACKLOG + ".trace"))
            return Level.FINEST;

        return Level.OFF;
    }

    private static Integer getDefaultTraceLength(String name) {
        if (name.equals(Constants.LOGGER_REPLICATION_BACKLOG + ".trace"))
            return 5000;

        return DEFAULT_TRACE_LENGTH;
    }

    public boolean isErrorEnabled() {
        return _customLogger.isLoggable(Level.SEVERE);
    }

    public void error(String msg) {
        _customLogger.log(Level.SEVERE, msg);
    }

    public void error(String msg, Throwable e) {
        _customLogger.log(Level.SEVERE, msg, e);
    }

    public boolean isWarnEnabled() {
        return _customLogger.isLoggable(Level.WARNING);
    }

    public void warn(String msg) {
        _customLogger.log(Level.WARNING, msg);
    }

    public void warn(String msg, Throwable e) {
        _customLogger.log(Level.WARNING, msg, e);
    }

    public boolean isDebugEnabled() {
        return _customLogger.isLoggable(Level.FINE);
    }

    public void debug(String msg) {
        _customLogger.log(Level.FINE, msg);
    }

    public void debug(String msg, Throwable e) {
        _customLogger.log(Level.FINE, msg, e);
    }

    public boolean isTraceEnabled() {
        return _customLogger.isLoggable(Level.FINEST);
    }

    public void trace(String msg) {
        _customLogger.log(Level.FINEST, msg);
    }

    private class CustomLogger extends Logger {
        CustomLogger(String name)         {
            super(name, null);
        }

        @Override
        public void log(LogRecord record) {
            _logger.log(record);

            if (record.getLevel().intValue() < _traceLoggingLevel || _traceLoggingLevel == OFF_VALUE)
                return;

            Integer threadTraceIndex = _threadLocalTraceIndex.get();
            //Log at thread local trace
            if (_threadLocalTraceEnabled) {
                _threadTrace.get()[(threadTraceIndex % _traceLength)] = record;
                _threadLocalTraceIndex.set(threadTraceIndex + 1);
            }
            //Log at global trace
            acquireAccess();
            try {
                _globalTrace[_globalTraceIndex.getAndIncrement() % _traceLength] = record;
            } finally {
                releaseAccess();
            }
        }
    }
}
