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

import com.gigaspaces.internal.jvm.JavaUtils;
import com.gigaspaces.start.SystemLocations;
import com.gigaspaces.start.XapNetworkInfo;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.logging.ErrorManager;
import java.util.logging.Formatter;
import java.util.logging.Handler;
import java.util.logging.Level;
import java.util.logging.LogManager;
import java.util.logging.LogRecord;
import java.util.logging.StreamHandler;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Rolling based logging {@link Handler}. <h3>Rollover:</h3> Rollover is based on two policies,
 * whichever policy is triggered first: <dl> <li>File size rolling policy (default 2MB)</li>
 * <li>Time based rolling policy (default is daily)</li> </dl> <h3>Properties:</h3>
 * <h4>formatter:</h4> The default log output {@link Formatter formatting} is given by {@link
 * GSSimpleFormatter}. <blockquote> Configuration: <code>com.gigaspaces.logger.RollingFileHandler.formatter
 * = com.gigaspaces.logger.GSSimpleFormatter</code> </blockquote> <h4>filename-pattern:</h4> The
 * file name pattern can be configured to include a placeholder for properties to resolve, such as
 * <tt>homedir</tt>, <tt>host</tt>, <tt>pid</tt> and <tt>date</tt>. Each placeholder is identified
 * by a set of curly brackets <code>{..}</code>. It may also be a placeholder for a custom defined
 * property. <blockquote>Configuration: <code>com.gigaspaces.logger.RollingFileHandler.filename-pattern
 * = {gs.logs}/logs/{date,yyyy-MM-dd~HH.mm}-gigaspaces-{service}-{host}-{pid}.log</code></blockquote>
 * <p/> A place holder value is that of an overriding system property. If no override was specified,
 * and the property is one of the predefined properties (i.e. homedir, host, pid, date), its value
 * is evaluated by the handler implementation. If the place holder is of a custom property, and no
 * value is defined, the propety is left as is. If any error occurs, it is reported and the property
 * is replaced by an empty string. Example override by system property: <blockquote>
 * <code>-Dcom.gigaspaces.logger.RollingFileHandler.filename-pattern.date=yyyy-MM-dd</code>
 * </blockquote> <h4>append:</h4> The append property specifies if output should be appended to an
 * existing file. Default is set to false. Thus, if a file already exists by this name, a unique
 * incrementing index to resolve the conflict will be concatenated. It will be added at the end of
 * the filename replacing ".log" with "__{unique}.log" or at the end of the filename if the pattern
 * doesn't end with ".log". <blockquote>Configuration: <code>com.gigaspaces.logger.RollingFileHandler.append
 * = false</code> </blockquote> <h4> size-rolling-policy:</h4> The file size rolling policy can be
 * configured to roll the file when a size limit is reached. It specifies an approximate maximum
 * amount to write (in bytes) to any one file. If this is zero, then there is no limit. If the
 * property is omitted, then a default of 2MB is assumed. <blockquote>Configuration:
 * <code>com.gigaspaces.logger.RollingFileHandler.size-rolling-policy = 2000000</code> </blockquote>
 * <h4>time-rolling-policy:</h4> The time based rolling policy can be configured to roll the file at
 * an occurring schedule. The time policy can be set to either one of: daily, weekly, monthly or
 * yearly. If the property is omitted, then the default pattern of "daily" is assumed; meaning daily
 * rollover (at midnight). For example, if "monthly" is configured, the file will rollover at the
 * beginning of each month. <blockquote>Configuration: <code>com.gigaspaces.logger.RollingFileHandler.time-rolling-policy
 * = daily</code> </blockquote> <h4>backup-policy:</h4> A backup-policy can be configured to backup
 * files. By default a {@link NullBackupPolicy} is configured, which does nothing. It can be
 * replaced by a {@link DeleteBackupPolicy} to keep a backup of files for a specified period. The
 * {@link BackupPolicy} interface allows custom implementations to be plugged-in.
 * <blockquote>Configuration:<br> <code>com.gigaspaces.logger.RollingFileHandler.backup-policy =
 * com.gigaspaces.logger.DeleteBackupPolicy</code> <br> <code>com.gigaspaces.logger.DeleteBackupPolicy.period
 * = 30</code> <br> <code>com.gigaspaces.logger.DeleteBackupPolicy.backup = 10</code> </blockquote>
 * <h4>debug-level:</h4> The debug level (configured to one of logging {@link Level}s) is the level
 * in which debug messages are displayed to the "standard" output stream. By default the level is
 * configured to "CONFIG" which displays the log file name. <blockquote> Configuration:
 * <code>com.gigaspaces.logger.RollingFileHandler.debug-level = CONFIG</code> </blockquote> <h3>
 * system property overrides</h3> Any of the logger properties can be configured by a system
 * property override, as specified by: <blockquote> <code>-Dcom.gigaspaces.logger.RollingFileHandler.[property-name]=[property-value]</code>
 * </blockquote> For example: <blockquote> <code>-Dcom.gigaspaces.logger.RollingFileHandler.debug-level=OFF</code>
 * </blockquote>
 *
 * @author Moran Avigdor
 * @see RollingFileHandlerConfigurer
 * @since 7.0
 */

public class RollingFileHandler extends StreamHandler {

    private static boolean monitorCreatedFiles = false;

    private static final List<File> filesCreated = new ArrayList<File>();

    public static void monitorCreatedFiles() {
        monitorCreatedFiles = true;
    }

    public static boolean isMonitoringCreatedFiles() {
        return monitorCreatedFiles;
    }

    public static File[] filesCreated() {
        return filesCreated.toArray(new File[filesCreated.size()]);
    }

    /**
     * configuration overrides to be queried if system property is absent
     */
    static final Properties overrides = new Properties();

    /**
     * Resolves to the full class name of this handler and a '.' at the end. Serves as the prefix
     * for handler properties in logging configuration file and as the prefix for system property
     * overrides.
     *
     * <code><pre>
     * logging configuration file:
     * com.gigaspaces.logger.RollingFileHandler.[property-name]=[property-value]
     *
     * System property:
     * -Dcom.gigaspaces.logger.RollingFileHandler.[property-name]=[property-value]
     * </pre></code>
     */
    protected final String HANDLER_PROP_PREFIX = getClass().getName() + ".";
    protected static final String LEVEL_PROP = "level";
    protected static final String FORMATTER_PROP = "formatter";
    protected static final String FILENAME_PATTERN_PROP = "filename-pattern";
    protected static final String SIZE_ROLLING_POLICY_PROP = "size-rolling-policy";
    protected static final String TIME_ROLLING_POLICY_PROP = "time-rolling-policy";
    protected static final String BACKUP_POLICY_PROP = "backup-policy";
    protected static final String APPEND_PROP = "append";
    protected static final String DEBUG_LEVEL = "debug-level";

    /**
     * Resolves to the full class name of this handler and the ".filename-pattern" property at the
     * end. Serves as the prefix for <tt>filename-pattern</tt> place holder system property
     * overrides.
     *
     * <code><pre>
     * -Dcom.gigaspaces.logger.RollingFileHandler.filename-pattern.[placeholder-name]=[placeholder-value]
     * </pre></code>
     */
    protected final String FILENAME_PATTTERN_PLACEHOLDER_PREFIX = HANDLER_PROP_PREFIX + FILENAME_PATTERN_PROP + ".";
    protected static final String HOMEDIR_PROP = "homedir";
    protected static final String LOGSDIR_PROP = "gs.logs";
    protected static final String HOST_PROP = "host";
    protected static final String PID_PROP = "pid";
    protected static final String SERVICE_PROP = "service";
    protected static final String DATE_PROP = "date";
    private static final Set<String> PATH_PROPERTIES = new HashSet<>(Arrays.asList(HOMEDIR_PROP, LOGSDIR_PROP));

    /* Match any property between {..} braces. */
    private static final Pattern FILE_PATTERN_PROPERTY_MATCHER = Pattern.compile("\\{([^\\{]*)\\}");

    private String filenamePattern;
    private SizeRollingPolicy sizeRollingPolicy;
    private TimeRollingPolicy timeRollingPolicy;
    private BackupPolicy backupPolicy;
    private boolean append;
    private Level debugLevel;

    /*
     * Configuration Defaults
     */
    protected static final String DATE_PATTERN_DEFAULT = "yyyy-MM-dd~HH.mm";
    protected static final String FILENAME_PATTERN_DEFAULT = "{gs.logs}/logs/{date," + DATE_PATTERN_DEFAULT + "}-gigaspaces-{service}-{host}-{pid}.log";
    protected static final int SIZE_ROLLING_POLICY_DEFAULT = 2000000; //2MB
    protected static final String TIME_ROLLING_POLICY_DEFAULT = "daily";
    protected static final String FILE_APPEND_DEFAULT = "false";
    protected static final String DEBUG_LEVEL_DEFAULT = "CONFIG";

    /* An output stream which could not be configured properly */
    private boolean corruptedOutputStream = false;

    /**
     * Construct a default <tt>RollingFileHandler</tt>. This will be configured entirely from
     * <tt>LogManager</tt> properties (or their default values).
     */
    public RollingFileHandler() {
        super();
        configure();
        configureOutputStream();
    }

    /**
     * Configures this logger using the properties provided by the {@link
     * LogManager#getProperty(String)} method. More specifically, configures the default ".level",
     * ".formatter", ".filename-pattern", ".size-rolling-policy", ".time-rolling-policy", and
     * ".append" properties. <p/> The configure method is called only once at construction, but
     * before any output stream is open. <p/> A user may wish to extend this handler and provide
     * additional configurations or overrides, before the output stream's file-name is generated.
     * For example, a user may implement a process ID extension that assigns the appropriate system
     * property with it's value, that will be used as the replacement for the default process ID
     * extractor, which relies on <code>ManagementFactory.getRuntimeMXBean().getName()</code>.
     */
    protected void configure() {
        LogManager manager = LogManager.getLogManager();

        // .level and .formatter are configured in super class.
        setDefaultLevelIfNull(resolveLoggerProperty(manager, LEVEL_PROP));
        setDefaultFormatterIfNull(resolveLoggerProperty(manager, FORMATTER_PROP));
        setFilenamePattern(resolveLoggerProperty(manager, FILENAME_PATTERN_PROP));
        setSizeRollingPolicy(resolveLoggerProperty(manager, SIZE_ROLLING_POLICY_PROP));
        setTimeRollingPolicy(resolveLoggerProperty(manager, TIME_ROLLING_POLICY_PROP));
        setBackupPolicy(resolveLoggerProperty(manager, BACKUP_POLICY_PROP));
        setFileAppend(resolveLoggerProperty(manager, APPEND_PROP));
        setDebugLevel(resolveLoggerProperty(manager, DEBUG_LEVEL));
    }

    /**
     * Resolve logger property by its name - system property value if available, or logger property
     * extracted from the log configuration.
     *
     * @param manager  The log manger to extract logger property configuration from
     * @param property The property name (without a '.' prefix)
     * @return the value of the property, <code>null</code> if not available.
     */
    private String resolveLoggerProperty(LogManager manager, String property) {
        String propertyKey = HANDLER_PROP_PREFIX + property;
        return System.getProperty(propertyKey, manager.getProperty(propertyKey));
    }

    /**
     * Sets the default level to {@link Level#ALL} if non was set; otherwise lets
     * <tt>StreamHandler#configure()</tt> to configure it for us.
     *
     * @param level one of the predefined {@link Level}s.
     */
    private void setDefaultLevelIfNull(String level) {
        if (level == null) {
            this.setLevel(Level.ALL);
        }
    }

    /**
     * Sets the default formatter if non was set; otherwise lets <tt>StreamHandler#configure()</tt>
     * to configure it for us.
     *
     * @param formatter fully qualified formatter class name or null.
     */
    private void setDefaultFormatterIfNull(String formatter) {
        if (formatter == null) {
            this.setFormatter(new GSSimpleFormatter());
        }
    }

    /**
     * Sets the filename pattern to use when generating file names (see {@link #generateFilename()}
     * ). If property wasn't set, use the default.
     */
    private void setFilenamePattern(String filenamePattern) {
        if (filenamePattern == null) {
            filenamePattern = FILENAME_PATTERN_DEFAULT;
        }
        filenamePattern = filenamePattern.replace("/", File.separator);
        this.filenamePattern = filenamePattern;
    }

    /**
     * Sets the file limit to use when peeking at the file size (see {@link #sizeRollingPolicy}). If
     * the property wasn't set, use the default 2MB.
     */
    private void setSizeRollingPolicy(String sizeLimit) {
        int limit = SIZE_ROLLING_POLICY_DEFAULT;
        if (sizeLimit != null) {
            limit = Integer.valueOf(sizeLimit).intValue();
            if (limit < 0) {
                limit = 0;
            }
        }
        this.sizeRollingPolicy = new SizeRollingPolicy(limit);
    }

    /**
     * Set the rollover time-based policy. The policy specifier may contain any one of the
     * time-boundaries as specified by the <tt>TimeRollingPolicy#TimeBoundry</tt> class. If the
     * property is omitted, then the default pattern of "daily" is assumed; meaning daily rollover
     * (at midnight).
     */
    private void setTimeRollingPolicy(String policy) {
        if (policy == null) {
            policy = TIME_ROLLING_POLICY_DEFAULT; // default: daily rollover policy
        }
        this.timeRollingPolicy = new TimeRollingPolicy(policy);
    }

    /**
     * Set the backup policy. If none was specified, uses the default {@link NullBackupPolicy}. If
     * an exception occurs trying to instantiate a user defined policy, uses the {@link
     * NullBackupPolicy}.
     *
     * @param policy an {@link BackupPolicy} implementation class name.
     */
    private void setBackupPolicy(String policy) {
        if (policy == null) {
            policy = NullBackupPolicy.class.getName();
        }
        try {
            Class<? extends BackupPolicy> backupPolicyClass = Class.forName(policy).asSubclass(BackupPolicy.class);
            BackupPolicy newInstance = backupPolicyClass.newInstance();
            this.backupPolicy = newInstance;
        } catch (Exception ex) {
            reportError("Can't create an instance of BackupPolicy class:" + policy, ex,
                    ErrorManager.GENERIC_FAILURE);
            this.backupPolicy = new NullBackupPolicy();
        }
    }

    /**
     * Sets the append property if we should append output to existing files.
     *
     * @param append "true" or "false"
     */
    private void setFileAppend(String append) {
        if (append == null) {
            append = FILE_APPEND_DEFAULT; // default: false
        }
        this.append = Boolean.parseBoolean(append);
    }

    /**
     * Sets the debug-level property specifying the level of debug messages.
     *
     * @param level one of the logger {@link Level}s
     */
    private void setDebugLevel(String level) {
        if (level == null) {
            level = DEBUG_LEVEL_DEFAULT;
        }
        this.debugLevel = Level.parse(level);
    }

    /**
     * Check if the debug-level allows a message to be logged at the specified level.
     *
     * @param level a message logging level
     * @return true if the given message level can be logged.
     */
    private boolean isDebuggable(Level level) {
        if (level.intValue() < debugLevel.intValue() || debugLevel == Level.OFF) {
            return false;
        }
        return true;
    }

    /**
     * Configure the output stream to use: 1. generate a filename 2. acquire a unique file (if a
     * filename by this name already exists) 3. open a stream 4. set the policies to monitor this
     * stream
     */
    private void configureOutputStream() {
        String filename = generateFilename();
        File file = null;
        try {
            file = acquireUniqueFile(filename);
            if (isDebuggable(Level.CONFIG)) {
                LogHelper.println("com.gigaspaces.logger", Level.CONFIG, "Log file: " + file.getAbsolutePath());
            }
            FileOutputStream fout = new FileOutputStream(file, append);
            BufferedOutputStream bout = new BufferedOutputStream(fout);
            MeteredStream meteredStream = new MeteredStream(bout, (int) file.length());
            setOutputStream(meteredStream); //meteredStream is closed using #closeMeteredStream()

            sizeRollingPolicy.setMeteredStream(meteredStream);
            timeRollingPolicy.setTimestamp();
            backupPolicy.track(file);
            if (monitorCreatedFiles) {
                filesCreated.add(0, file);
            }
        } catch (IOException ioe) {
            String filepath = file != null ? file.getAbsolutePath() : filename;
            reportError("Failed while configuring output file: " + filepath, ioe, ErrorManager.OPEN_FAILURE);
            corruptedOutputStream = true;
            if (sizeRollingPolicy != null) {
                sizeRollingPolicy.closeMeteredStream();
            }
        }
    }

    /**
     * Generates a filename based on the <tt>filename-pattern</tt> given. Replaces any named
     * property between {..}, with it's system property value if available, or if the named property
     * is known, with it's generated value. If a value could not be determined, the named property
     * is left as is, omitting the {..} braces.
     *
     * @return a filename
     */
    private String generateFilename() {
        String filename = filenamePattern;
        Matcher matcher = FILE_PATTERN_PROPERTY_MATCHER.matcher(filename);
        while (matcher.find()) {
            String propertyName = matcher.group(1);
            String propertyValue = resolveProperty(propertyName);

            if (propertyValue == null) {
                reportError("Could not find system property specified in log filename pattern '" + propertyName + "'",
                        new RuntimeException(), ErrorManager.FORMAT_FAILURE);

            } else {
                if (!PATH_PROPERTIES.contains(propertyName)) {
                    propertyValue = removeIllegalFileCharacters(propertyValue);
                }
                filename = filename.substring(0, matcher.start()) + propertyValue + filename.substring(matcher.end());
                matcher.reset(filename);
            }
        }
        return filename;
    }

    /**
     * Resolve a property by its name - system property value if available, or if the named property
     * is known (i.e. host, pid, date), with it's generated value. If a value could not be
     * determined, the property name is returned. If an error occurs, it is logged and an empty
     * string is returned.
     */
    private String resolveProperty(String propertyName) {
        try {
            String propertyValue = System.getProperty(FILENAME_PATTTERN_PLACEHOLDER_PREFIX + propertyName);
            if (propertyValue != null) {
                return propertyValue;
            }
            propertyValue = overrides.getProperty(propertyName);
            if (propertyValue != null) {
                return propertyValue;
            }
            if (propertyName.equals(HOMEDIR_PROP)) {
                return SystemLocations.singleton().home().toString();
            } else if (propertyName.equals(LOGSDIR_PROP)) {
                return SystemLocations.singleton().logs().toString();
            } else if (propertyName.equals(HOST_PROP)) {
                return XapNetworkInfo.getInstance().getHostId();
            } else if (propertyName.equals(PID_PROP)) {
                return "" + JavaUtils.findProcessId();
            } else if (propertyName.startsWith(DATE_PROP)) {
                return getCurrDate(propertyName);
            } else {
                return propertyName;
            }
        } catch (Throwable t) {
            // protect against any unexpected failure - log it and return empty string.
            reportError("Failed acquiring property value for: " + propertyName, new Exception(t),
                    ErrorManager.FORMAT_FAILURE);
            return "";
        }
    }

    private static String getCurrDate(String propertyName) {
        int indexOf = propertyName.indexOf(',');
        String dateFormat = indexOf != -1 ? propertyName.substring(indexOf + 1, propertyName.length()) : DATE_PATTERN_DEFAULT;
        return new SimpleDateFormat(dateFormat).format(new Date());
    }

    /**
     * Removes all illegal file characters <tt>/\:*?"<>|</tt>
     *
     * @param string to parse
     * @return a string with all illegal characters removed.
     */
    private String removeIllegalFileCharacters(String string) {
        return string.replaceAll("/|\\\\|:|\\*|\\?|\"|<|>|\\||\\s", "");
    }

    /**
     * Acquire a unique filename - use the original file name if possible, but if the file already
     * exists, try and assign a unique incrementing index. If the 'append' property is enabled, then
     * the existing file will be used.
     *
     * @param originalFilename the filename to acquire if possible
     * @return a unique File instance to the acquired file.
     * @throws IOException If an I/O error occurred
     */
    private File acquireUniqueFile(final String originalFilename) throws IOException {
        String filename = originalFilename;
        int unique = 0;
        for (; ; ) {
            File f = new File(filename);
            createParentDirectories(f);
            boolean created = f.createNewFile();
            if (created) {
                return f;
            } else if (append && !sizeRollingPolicy.hasReachedLimit(f)) {
                return f;
            } else {
                ++unique;
                if (originalFilename.endsWith(".log")) {
                    filename = originalFilename.replace(".log", "__" + unique + ".log");
                } else {
                    filename = originalFilename + "__" + unique;
                }
            }
        }
    }

    /**
     * Create all directories parent to this file.
     */
    private void createParentDirectories(File file) {
        if (!file.exists()) {
            String path = file.getAbsolutePath();
            String dirs = path.substring(0, path.lastIndexOf(File.separator));
            File dir = new File(dirs);
            if (dir.exists()) {
                return;
            }
            if (!dir.mkdirs()) {
                reportError("Failed to create directories: " + dir.getAbsolutePath(), new IOException(
                        "The system cannot find the path specified: " + dir.getAbsolutePath()), ErrorManager.OPEN_FAILURE);
            }
        }
    }

    /**
     * Applies the policies before writing to the stream; re-configures the stream if policy is
     * triggered.
     *
     * @see java.util.logging.StreamHandler#publish(java.util.logging.LogRecord)
     */
    @Override
    public synchronized void publish(LogRecord record) {
        if (corruptedOutputStream) {
            return;
        }

        if (sizeRollingPolicy.hasReachedLimit() || timeRollingPolicy.needsRollover()) {
            sizeRollingPolicy.closeMeteredStream();
            configureOutputStream();
        }
        super.publish(record);
        super.flush();
    }

    @Override
    public synchronized void close() throws SecurityException {
        super.close();

        if (sizeRollingPolicy != null) {
            sizeRollingPolicy.closeMeteredStream();
        }
    }
}
