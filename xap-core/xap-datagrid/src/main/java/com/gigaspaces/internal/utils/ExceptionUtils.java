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
package com.gigaspaces.internal.utils;


import java.io.PrintWriter;
import java.io.StringWriter;

/**
 * Utility methods to work with exceptions.
 *
 * @author moran
 * @since 6.6
 */
public class ExceptionUtils {

    /**
     * Returns the nested failure's which caused this exception to occur.
     * @param t The exception that occurred.
     * @return  a "Nested cause:" concatenation.
     */
    public static String getNestedFailureReasons(Throwable t) {
        String failure = "";
        Throwable cause = t;
        while (cause != null) {
            if (failure != "") {
                failure += "; Nested cause: ";
            }
            failure += cause.toString();
            cause = cause.getCause();
        }

        return failure;
    }

    /**
     * Concatenates Throwable.getMessage() and all "Caused By" reasons from this Throwable into a flat String.
     * e.g. returns Throwable.getMessage(); Caused By: java.lang.Exception: third level; Caused By: java.lang.Exception: second level; Caused By: java.lang.Exception: first level
     */
    public static String getRecursiveCause(Throwable t)
    {
        StringBuilder result = new StringBuilder(t.getMessage());
        Throwable cause = t;
        while (cause.getCause() != null) {
            cause = cause.getCause();
            result.append("; Caused By: " + cause.toString());
        }

        return result.toString();
    }

    /**
     * returns the current stack trace in a String format
     */
    public static String getCurrentStackTrace() {
        Throwable e = new Throwable();
        StringWriter sw = new StringWriter();
        PrintWriter pw = new PrintWriter(sw);
        e.printStackTrace(pw);
        return sw.toString();
    }
}