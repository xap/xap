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

    private static final String INDENTATION = "  ";
    private final StringBuilder sb;
    private String currentIndentation = "";
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
        sb.append(currentIndentation);
        if (firstLinePrefix != null) {
            sb.append(firstLinePrefix).append(" ");
            currentIndentation = currentIndentation + String.join("", Collections.nCopies(firstLinePrefix.length() + 1, " "));
            firstLinePrefix = null;
        }
        sb.append(s);
        sb.append(StringUtils.NEW_LINE);
        return this;
    }

    public TextReportFormatter indent() {
        currentIndentation = currentIndentation + INDENTATION;
        return this;
    }

    public TextReportFormatter unindent() {
        currentIndentation = currentIndentation.substring(0, currentIndentation.length() - INDENTATION.length());
        return this;
    }

    public void indent(Runnable function) {
        indent();
        function.run();
        unindent();
    }

    public void withFirstLine(String firstLinePrefix, Runnable function) {
        String orgIndenation = currentIndentation;
        this.firstLinePrefix = firstLinePrefix;
        function.run();
        this.currentIndentation = orgIndenation;
    }

    public void withPrefix(String prefix, Runnable function) {
        String orgIndentation = currentIndentation;
        currentIndentation = currentIndentation + prefix;
        function.run();
        currentIndentation = orgIndentation;

    }

}
