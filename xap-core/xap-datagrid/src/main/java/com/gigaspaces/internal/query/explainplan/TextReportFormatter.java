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

import java.util.List;

/**
 * @author Niv Ingberg
 * @since 12.0.1
 */
public class TextReportFormatter {
    private final StringBuilder sb;
    private int indents;

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
        for (int i=0 ; i < indents ; i++)
            sb.append('\t');
        sb.append(s);
        sb.append(StringUtils.NEW_LINE);
        return this;
    }

    public TextReportFormatter inline(String s) {
        sb.append(s);
        return this;
    }

    public TextReportFormatter inline(List<String> list) {
        for (String s : list) {
            inline(s);
        }
        return this;
    }

    public TextReportFormatter line(List<String> strings) {
        int i = 0;
        indent();
        if (strings.size() > 1) {
            line(strings.get(0));
            ++i;
            indent();
        }

        for (;i < strings.size(); i++) {
            line(strings.get(i));
        }

        if (strings.size()> 1) {
            unindent();
        }
        unindent();
        return this;
    }

    public TextReportFormatter indent() {
        indents++;
        return this;
    }

    public TextReportFormatter unindent() {
        indents--;
        return this;
    }

    public TextReportFormatter newLine() {
        sb.append(StringUtils.NEW_LINE);
        for (int i=0 ; i < indents ; i++)
            sb.append('\t');
        return this;
    }
}
