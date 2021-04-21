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

/**
 * @author Niv Ingberg
 * @since 12.0.1
 */
public class TextReportFormatter {
    private final StringBuilder sb;
    private int indents;
    private String firstLinePrefix;
    private String prefix;
    private int prefixPosition;

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
        System.out.println();
        System.out.println("Writing ["+s+"] with ["+indents+"] indentations");
        if (prefix != null) {
            for (int i = 0; i < prefixPosition-1; i++) {
                sb.append(" ");
            }
            sb.append(prefix);
            for (int i = 0; i < indents - prefixPosition -1; i++) {
                sb.append(" ");
            }
        } else {
            System.out.println("Will add ["+(indents)+"] indentations to the start of the line");
            for (int i = 0; i < indents; i++)
                sb.append(" ");
        }
        if (firstLinePrefix != null) {
            sb.append(firstLinePrefix).append(" ");
            indents+= (firstLinePrefix.length() + 1);
            System.out.println("Adding ["+(firstLinePrefix.length() + 1)+"] indentation. Current: "+ indents);
            firstLinePrefix = null;
        }
        sb.append(s);
        sb.append(StringUtils.NEW_LINE);
        return this;
    }

    public TextReportFormatter indent() {
        indents+=2;
        return this;
    }

    public TextReportFormatter unindent() {
        indents-=2;
        return this;
    }

    public void indent(Runnable function) {
        indent();
        function.run();
        unindent();
    }
//    public void indent(String s, Runnable function) {
//        indent();
//        firstLinePrefix = s;
//        function.run();
//        unindent();
//        indents-= (s.length() + 1);
//    }

//    public void line(String s, String format) {
//        firstLinePrefix = s;
//        line(format);
//    }

//    public void setFirstLinePrefix(String prefix) {
//        this.firstLinePrefix = prefix;
//    }

    public void withFirstLine(String firstLinePrefix, Runnable function) {
        this.firstLinePrefix = firstLinePrefix;
        function.run();
        this.firstLinePrefix = null;
        indents -= (firstLinePrefix.length() + 1);
        System.out.println("Removing ["+(firstLinePrefix.length() + 1)+"] indentations. current: " + indents);
    }

    public void withPrefix(String prefix, Runnable function) {
        this.prefix = prefix;
        int org = indents;
        prefixPosition= indents + 1;
        function.run();
        prefixPosition = org;
        this.prefix = null;

    }
}
