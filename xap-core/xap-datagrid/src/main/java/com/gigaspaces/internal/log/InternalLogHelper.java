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

package com.gigaspaces.internal.log;

import com.gigaspaces.internal.io.BootIOUtils;
import com.gigaspaces.internal.os.OSDetails;
import com.gigaspaces.internal.os.OSHelper;
import com.gigaspaces.log.ClientLogEntryMatcherCallback;
import com.gigaspaces.log.CompoundLogEntries;
import com.gigaspaces.log.LogEntries;
import com.gigaspaces.log.LogEntry;
import com.gigaspaces.log.LogEntryMatcher;
import com.gigaspaces.log.LogProcessType;
import com.gigaspaces.logger.RollingFileHandler;
import com.gigaspaces.start.SystemInfo;

import java.io.BufferedReader;
import java.io.File;
import java.io.FilenameFilter;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.RandomAccessFile;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.Date;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.ListIterator;
import java.util.Map;

/**
 * @author kimchy
 */
@com.gigaspaces.api.InternalApi
public class InternalLogHelper {

    private static String lineSeparator = System.getProperty("line.separator");

    public static LogProcessType parseProcessTypeFromSystemProperty() {
        String logFileName = System.getProperty("gs.logFileName");
        if (logFileName != null) {
            logFileName = logFileName.toUpperCase();
            for (LogProcessType logProcessType : LogProcessType.values()) {
                if (logFileName.contains(logProcessType.name())) {
                    return logProcessType;
                }
            }
        }

        return null;
    }

    public static LogEntries logEntries(final LogProcessType type, final long pid, LogEntryMatcher matcher) throws IOException {
        if (!RollingFileHandler.isMonitoringCreatedFiles()) {
            throw new IOException("Logger not monitoring created files...");
        }
        File logDir = RollingFileHandler.filesCreated()[0].getParentFile();
        File[] logFiles = BootIOUtils.listFiles(logDir, new FilenameFilter() {
            @Override
            public boolean accept(File dir, String name) {

                String nameInLowerCase = name.toLowerCase();
                String typeNameInLowerCase = type.name().toLowerCase();
                return nameInLowerCase.contains("-" + pid) ||
                        nameInLowerCase.contains("_" + pid) &&
                                (nameInLowerCase.contains("_" + typeNameInLowerCase) ||
                                        nameInLowerCase.contains("-" + typeNameInLowerCase));
            }
        });

        if (logFiles.length == 0) {
            OSDetails osDetails = OSHelper.getDetails();
            return new LogEntries(type, pid, osDetails.getHostName(), osDetails.getHostAddress());
        }

        Arrays.sort(logFiles, new NewestToOldestFileComparator());
        return logEntriesDirect(logFiles, pid, type, matcher);
    }

    public static CompoundLogEntries logEntries(final LogProcessType[] types, final long[] pids, LogEntryMatcher matcher) throws IOException {
        LogEntries[] logEntries = new LogEntries[types.length];
        for (int i = 0; i < types.length; i++) {
            logEntries[i] = logEntries(types[i], pids[i], matcher);
        }
        return new CompoundLogEntries(logEntries);
    }

    public static CompoundLogEntries logEntries(final LogProcessType type, LogEntryMatcher matcher) throws IOException {
        if (!RollingFileHandler.isMonitoringCreatedFiles()) {
            throw new IOException("Logger not monitoring created files...");
        }
        File logDir = RollingFileHandler.filesCreated()[0].getParentFile();
        File[] logFiles = BootIOUtils.listFiles(logDir, new FilenameFilter() {
            public boolean accept(File dir, String name) {
                return name.toLowerCase().contains("-" + type.name().toLowerCase());
            }
        });
        if (logFiles == null || logFiles.length == 0) {
            return new CompoundLogEntries(new LogEntries[0]);
        }
        Map<Long, List<File>> typePerPid = new HashMap<Long, List<File>>();
        for (File file : logFiles) {
            long pid = extractPid(file);
            List<File> list = typePerPid.get(pid);
            if (list == null) {
                list = new ArrayList<File>();
                typePerPid.put(pid, list);
            }
            list.add(file);
        }
        List<LogEntries> logEntries = new ArrayList<LogEntries>();
        for (Map.Entry<Long, List<File>> entry : typePerPid.entrySet()) {
            logEntries.add(logEntriesDirect(entry.getValue().toArray(new File[entry.getValue().size()]), entry.getKey(), type, matcher));
        }
        LogEntries[] result = logEntries.toArray(new LogEntries[logEntries.size()]);
        Arrays.sort(result, new Comparator<LogEntries>() {
            public int compare(LogEntries o1, LogEntries o2) {
                return (int) (o1.getLatestLogFileTimestamp() - o2.getLatestLogFileTimestamp());
            }
        });
        return new CompoundLogEntries(result);
    }

    private static long extractPid(File file) {
        int lastDashIdx = file.getName().lastIndexOf('-');
        int dotIdx = file.getName().indexOf('.', lastDashIdx);
        return Long.parseLong(file.getName().substring(lastDashIdx + 1, dotIdx));
    }

    private static class NewestToOldestFileComparator implements Comparator<File> {
        public int compare(File o1, File o2) {
            if (o1.lastModified() > o2.lastModified()) {
                return -1;
            }
            if (o1.lastModified() == o2.lastModified()) {
                return 0;
            }
            return 1;
        }
    }

    public static LogEntries logEntriesDirect(LogProcessType type, LogEntryMatcher matcher) throws IOException {
        if (!RollingFileHandler.isMonitoringCreatedFiles()) {
            throw new IOException("Logger not monitoring created files...");
        }
        File[] files = RollingFileHandler.filesCreated();
        return logEntriesDirect(files, SystemInfo.singleton().os().processId(), type, matcher);
    }

    public static File[] logFiles() throws IOException {
        if (!RollingFileHandler.isMonitoringCreatedFiles()) {
            throw new IOException("Logger not monitoring created files...");
        }
        return RollingFileHandler.filesCreated();
    }

    /**
     * Retrieves the log entries. Note, files should be from newest to oldest proder ([0] is the
     * newest/latest log file).
     */
    public static LogEntries logEntriesDirect(File[] files, long pid, LogProcessType type, LogEntryMatcher matcher) throws IOException {
        SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss,SSS");
        OSDetails osDetails = OSHelper.getDetails();
        matcher.initialize(new LogEntryMatcher.InitializationContext(osDetails.getHostAddress(), osDetails.getHostName(), pid, type));
        StringBuilder sb = new StringBuilder();
        File latestFile = files[0];
        for (int fileIndex = 0; fileIndex < files.length; fileIndex++) {
            File file = files[fileIndex];
            ArrayList<String> lines = new ArrayList<>();
            try (BackwardsFileInputStream fileInputStream = new BackwardsFileInputStream(file)) {
                try (BufferedReader reader = new BufferedReader(new InputStreamReader(fileInputStream))) {
                    String line;
                    LogEntryMatcher.Operation operation = matcher.match(new LogEntry(files.length - 1 - fileIndex, LogEntry.Type.FILE_MARKER, file.lastModified(), file.getAbsolutePath()));
                    if (operation == LogEntryMatcher.Operation.BREAK) {
                        break;
                    }
                    if (operation == LogEntryMatcher.Operation.IGNORE) {
                        continue;
                    }
                    boolean firstLine = true;
                    long position = fileInputStream.position() + lineSeparator.length();
                    while ((line = reader.readLine()) != null) {
                        if (firstLine) {
                            if (line.length() == 0) {
                                continue;
                            }
                            firstLine = false;
                        }
                        char[] chars = line.toCharArray();
                        for (int j = 0, k = chars.length - 1; j < k; j++, k--) {
                            char temp = chars[j];
                            chars[j] = chars[k];
                            chars[k] = temp;
                        }
                        position -= chars.length + lineSeparator.length();
                        line = new String(chars);
                        int idx = line.indexOf(' ');
                        if (idx == -1) {
                            lines.add(line);
                            continue;
                        }
                        idx = line.indexOf(' ', idx + 1);
                        if (idx == -1) {
                            lines.add(line);
                            continue;
                        }
                        Date timestamp;
                        try {
                            timestamp = dateFormat.parse(line.substring(0, idx));
                        } catch (ParseException e) {
                            lines.add(line);
                            continue;
                        }
                        // we got a log entry
                        String logText;
                        if (lines.isEmpty()) {
                            logText = line;
                        } else {
                            sb.setLength(0);
                            sb.append(line).append(lineSeparator);
                            for (int i = lines.size() - 1; i >= 0; i--) {
                                if (i == 0 && lines.get(i).length() == 0) {
                                    break;
                                }
                                sb.append(lines.get(i));
                                if (i != 0) {
                                    sb.append(lineSeparator);
                                }
                            }
                            logText = sb.toString();
                        }
                        if (matcher.match(new LogEntry(position, LogEntry.Type.LOG, timestamp.getTime(), logText)) == LogEntryMatcher.Operation.BREAK) {
                            return toEntries(type, pid, files, matcher.entries(), latestFile.lastModified());
                        }
                        lines.clear();
                    }
                }
            }
        }
        return toEntries(type, pid, files, matcher.entries(), latestFile.lastModified());
    }

    public static LogEntries clientSideProcess(LogEntryMatcher matcher, LogEntries entries) {
        if (matcher instanceof ClientLogEntryMatcherCallback) {
            return ((ClientLogEntryMatcherCallback) matcher).clientSideProcess(entries);
        }
        return entries;
    }

    private static LogEntries toEntries(LogProcessType type, long pid, File[] files, List<LogEntry> entries, long latestLogTimestamp) {
        LinkedList<LogEntry> reveredEntries = new LinkedList<LogEntry>();
        LogEntry fileMarker = null;
        for (ListIterator<LogEntry> it = entries.listIterator(entries.size()); it.hasPrevious(); ) {
            LogEntry entry = it.previous();
            if (entry.isFileMarker()) {
                if (fileMarker != null) {
                    reveredEntries.addFirst(fileMarker);
                }
                fileMarker = entry;
            } else {
                reveredEntries.add(entry);
            }
        }
        if (fileMarker != null) {
            reveredEntries.addFirst(fileMarker);
        }
        OSDetails osDetails = OSHelper.getDetails();
        return new LogEntries(type, reveredEntries, files.length, pid, latestLogTimestamp, osDetails.getHostName(), osDetails.getHostAddress());
    }

    static public class BackwardsFileInputStream extends InputStream {
        public BackwardsFileInputStream(File file) throws IOException {
            assert (file != null) && file.exists() && file.isFile() && file.canRead();

            raf = new RandomAccessFile(file, "r");
            currentPositionInFile = raf.length();
            currentPositionInBuffer = 0;
        }

        public int read() throws IOException {
            if (currentPositionInFile <= 0)
                return -1;
            if (--currentPositionInBuffer < 0) {
                currentPositionInBuffer = buffer.length;
                long startOfBlock = currentPositionInFile - buffer.length;
                if (startOfBlock < 0) {
                    currentPositionInBuffer = buffer.length + (int) startOfBlock;
                    startOfBlock = 0;
                }
                raf.seek(startOfBlock);
                raf.readFully(buffer, 0, currentPositionInBuffer);
                return read();
            }
            currentPositionInFile--;
            byte retVal = buffer[currentPositionInBuffer];
            if (retVal == '\r') {
                // ignore carriage returns, just rely on "\n" which is cross platform
                return read();
            }
            return retVal;
        }

        public long position() {
            return currentPositionInFile + currentPositionInBuffer;
        }

        public void close() throws IOException {
            raf.close();
        }

        private final byte[] buffer = new byte[1024 * 16];
        private final RandomAccessFile raf;
        private long currentPositionInFile;
        private int currentPositionInBuffer;
    }
}
