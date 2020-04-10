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

package com.gigaspaces.metrics.reporters;

import com.gigaspaces.metrics.MetricGroupSnapshot;
import com.gigaspaces.metrics.MetricRegistrySnapshot;
import com.gigaspaces.metrics.MetricReporter;
import com.gigaspaces.metrics.MetricTagsSnapshot;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author yohana
 * @since 10.1
 */
@com.gigaspaces.api.InternalApi
public class FileReporter extends MetricReporter {
    private static final Logger logger = LoggerFactory.getLogger(FileReporter.class.getName());
    private static final String NEWLINE = System.getProperty("line.separator");

    private final Date date = new Date();
    private final SimpleDateFormat dateFormatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss,SSS");
    private final File file;
    private final StringBuilder buffer;

    protected FileReporter(FileReporterFactory factory) {
        super(factory);
        File file = new File(factory.getPath());
        //noinspection ResultOfMethodCallIgnored
        file.getParentFile().mkdirs();
        this.file = file.getAbsoluteFile();
        this.buffer = new StringBuilder();
    }

    @Override
    public void report(List<MetricRegistrySnapshot> snapshots) {
        buffer.append("Reported at ").append(formatDateTime(System.currentTimeMillis())).append(NEWLINE);
        super.report(snapshots);
        writeToFile(buffer.toString());
        buffer.setLength(0);
    }

    @Override
    protected void report(MetricRegistrySnapshot snapshot) {
        buffer.append("Sample taken at ").append(formatDateTime(snapshot.getTimestamp())).append(NEWLINE);
        super.report(snapshot);
    }

    @Override
    protected void report(MetricRegistrySnapshot snapshot, MetricTagsSnapshot tags, MetricGroupSnapshot group) {
        buffer.append("\tTags: ").append(tags).append(NEWLINE);
        super.report(snapshot, tags, group);
    }

    @Override
    protected void report(MetricRegistrySnapshot snapshot, MetricTagsSnapshot tags, String key, Object value) {
        buffer.append("\t\t").append(key).append(" => ").append(value).append(NEWLINE);
    }

    private String formatDateTime(long timestamp) {
        date.setTime(timestamp);
        return dateFormatter.format(date);
    }

    private void writeToFile(String text) {
        FileWriter fw = null;
        try {
            fw = new FileWriter(file, true);
            fw.write(text);
        } catch (IOException e) {
            logger.warn("Failed to write report to file", e);
        }
        if (fw != null) {
            try {
                fw.close();
            } catch (IOException e) {
                logger.warn("Failed to close FileWriter", e);
            }
        }
    }
}
