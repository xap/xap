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
package com.gigaspaces.internal.query.explainplan;

import com.gigaspaces.internal.utils.StringUtils;

import java.util.Collections;

/**
 * @author Niv Ingberg
 * @since 12.0.1
 */
public class TextReportFormatter {

    private final StringBuilder sb;
    private int prefixPosition;

    private String indentation = "";
    private String firstLinePrefix;

    public TextReportFormatter() {
        this(new StringBuilder());
    }

    public TextReportFormatter(StringBuilder sb) {
        this.sb = sb;
    }

    public String toString() {
        return sb.toString();
    }

    public TextReportFormatter line(String s) {
        sb.append(indentation);
        if (firstLinePrefix != null) {
            sb.append(firstLinePrefix).append(" ");
            indentation = indentation + String.join("", Collections.nCopies(firstLinePrefix.length() + 1, " "));
            firstLinePrefix = null;
        }
        sb.append(s);
        sb.append(StringUtils.NEW_LINE);
        return this;
    }

    public TextReportFormatter indent() {
        indentation = indentation+"  ";//2 spaces
        return this;
    }

    public TextReportFormatter unindent() {
        indentation = indentation.substring(indentation.length() - 1);
        return this;
    }

    public void indent(Runnable function) {
        indent();
        function.run();
        unindent();
    }

    public void withFirstLine(String firstLinePrefix, Runnable function) {
        String orgIndenation = indentation;
        this.firstLinePrefix = firstLinePrefix;
        function.run();
        this.indentation = orgIndenation;
    }

    public void withPrefix(String prefix, Runnable function) {
        String orgIndentation = indentation;
        indentation = indentation + prefix;
        function.run();
        indentation = orgIndentation;

    }
}
