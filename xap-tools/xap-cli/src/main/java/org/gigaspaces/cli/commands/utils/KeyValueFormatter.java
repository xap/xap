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
package org.gigaspaces.cli.commands.utils;

/**
 * @author Niv Ingberg
 * @since 14.5
 */
public class KeyValueFormatter {
    private final StringBuilder sb = new StringBuilder();
    private final String format;

    private KeyValueFormatter(Builder builder) {
        this.format = "%-" + builder.width + "s" + builder.separator + "%s%n";
    }

    public KeyValueFormatter append(String key, Object value) {
        sb.append(String.format(format, key, value));
        return this;
    }

    public KeyValueFormatter append(String s) {
        sb.append(s);
        return this;
    }

    public KeyValueFormatter appendLineSeparator() {
        return append(System.lineSeparator());
    }

    public String get() {
        return sb.toString();
    }

    public static Builder builder() {
        return new Builder();
    }

    public static class Builder {
        private int width = 16;
        private String separator = " ";

        public Builder width(int width) {
            this.width = width;
            return this;
        }

        public Builder separator(String separator) {
            this.separator = separator;
            return this;
        }

        public KeyValueFormatter build() {
            return new KeyValueFormatter(this);
        }
    }
}
