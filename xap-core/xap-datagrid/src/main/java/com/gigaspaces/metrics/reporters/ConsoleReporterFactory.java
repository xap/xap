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

import com.gigaspaces.metrics.MetricReporterFactory;

import java.io.IOException;
import java.io.OutputStream;
import java.io.PrintStream;
import java.util.Locale;
import java.util.TimeZone;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author Niv Ingberg
 * @since 10.1
 */

public class ConsoleReporterFactory extends MetricReporterFactory<ConsoleReporter> {

    private PrintStream output;
    private Locale locale;
    private TimeZone timeZone;

    public ConsoleReporterFactory() {
        setOutput(System.out);
        setLocale(Locale.getDefault());
        setTimeZone(TimeZone.getDefault());
    }

    @Override
    public ConsoleReporter create() {

        final Logger logger = LoggerFactory.getLogger(ConsoleReporterFactory.class.getName());
        if (logger.isDebugEnabled()) {
            setLoggerAsOutputStream(logger);
        }

        return new ConsoleReporter(this);
    }

    private void setLoggerAsOutputStream(final Logger logger) {
        setOutput(new PrintStream(new OutputStream() {
            StringBuilder s = new StringBuilder();

            @Override
            public void write(int b) throws IOException {
                s.append((char) b);
            }

            @Override
            public void flush() throws IOException {
                logger.debug(s.toString());
                s.setLength(0);
            }
        }));
    }

    public PrintStream getOutput() {
        return output;
    }

    public void setOutput(PrintStream output) {
        this.output = output;
    }

    public Locale getLocale() {
        return locale;
    }

    public void setLocale(Locale locale) {
        this.locale = locale;
    }

    public TimeZone getTimeZone() {
        return timeZone;
    }

    public void setTimeZone(TimeZone timeZone) {
        this.timeZone = timeZone;
    }
}
