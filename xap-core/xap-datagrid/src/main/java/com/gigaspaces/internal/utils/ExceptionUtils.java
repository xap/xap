package com.gigaspaces.internal.utils;

/**
 * Created by ester on 11/01/2018.
 */

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