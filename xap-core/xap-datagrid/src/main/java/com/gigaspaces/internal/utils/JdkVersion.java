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

public abstract class JdkVersion {

    /**
     * Constant identifying the 1.3.x JVM (JDK 1.3).
     */
    public static final int JAVA_3 = 3;

    /**
     * Constant identifying the 1.4.x JVM (J2SE 1.4).
     */
    public static final int JAVA_4 = 4;

    /**
     * Constant identifying the 1.5 JVM (Java 5).
     */
    public static final int JAVA_5 = 5;

    /**
     * Constant identifying the 1.6 JVM (Java 6).
     */
    public static final int JAVA_6 = 6;

    /**
     * Constant identifying the 1.7 JVM (Java 7).
     */
    public static final int JAVA_7 = 7;

    /**
     * Constant identifying the 1.8 JVM (Java 8).
     */
    public static final int JAVA_8 = 8;

    /**
     * Constant identifying the 1.9 JVM (Java 9).
     */
    public static final int JAVA_9 = 9;

    /**
     * Constant identifying the 1.10 JVM (Java 10).
     */
    public static final int JAVA_10 = 10;

    /**
     * Constant identifying the 1.11 JVM (Java 10).
     */
    public static final int JAVA_11 = 11;


    private static final String javaVersion;

    private static final int majorJavaVersion;

    static {
        javaVersion = System.getProperty("java.version");
        // version String should look like "1.4.2_10"
        if (javaVersion.indexOf("11.") != -1) {
            majorJavaVersion = JAVA_11;
        } else if (javaVersion.indexOf("10.") != -1) {
            majorJavaVersion = JAVA_10;
        } else if (javaVersion.indexOf("9.") != -1) {
            majorJavaVersion = JAVA_9;
        } else if (javaVersion.indexOf("1.8.") != -1) {
            majorJavaVersion = JAVA_8;
        } else if (javaVersion.indexOf("1.7.") != -1) {
            majorJavaVersion = JAVA_7;
        } else if (javaVersion.indexOf("1.6.") != -1) {
            majorJavaVersion = JAVA_6;
        } else if (javaVersion.indexOf("1.5.") != -1) {
            majorJavaVersion = JAVA_5;
        } else {
            // else leave 1.4 as default (it's either 1.4 or unknown)
            majorJavaVersion = JAVA_4;
        }
    }


    /**
     * Return the full Java version string, as returned by <code>System.getProperty("java.version")</code>.
     *
     * @return the full Java version string
     * @see System#getProperty(String)
     */
    public static String getJavaVersion() {
        return javaVersion;
    }

    /**
     * Get the major version code. This means we can do things like <code>if (getMajorJavaVersion()
     * < JAVA_4)</code>.
     *
     * @return a code comparable to the JAVA_XX codes in this class
     * @see #JAVA_3
     * @see #JAVA_4
     * @see #JAVA_5
     * @see #JAVA_6
     * @see #JAVA_7
     * @see #JAVA_8
     * @see #JAVA_9
     */
    public static int getMajorJavaVersion() {
        return majorJavaVersion;
    }

    /**
     * Convenience method to determine if the current JVM is at least Java 1.4.
     *
     * @return <code>true</code> if the current JVM is at least Java 1.4
     * @see #getMajorJavaVersion()
     * @see #JAVA_4
     * @see #JAVA_5
     * @see #JAVA_6
     * @see #JAVA_7
     * @see #JAVA_8
     * @see #JAVA_9
     */
    public static boolean isAtLeastJava4() {
        return true;
    }

    /**
     * Convenience method to determine if the current JVM is at least Java 1.5 (Java 5).
     *
     * @return <code>true</code> if the current JVM is at least Java 1.5
     * @see #getMajorJavaVersion()
     * @see #JAVA_5
     * @see #JAVA_6
     * @see #JAVA_7
     * @see #JAVA_8
     * @see #JAVA_9
     */
    public static boolean isAtLeastJava5() {
        return getMajorJavaVersion() >= JAVA_5;
    }

    /**
     * Convenience method to determine if the current JVM is at least Java 1.6 (Java 6).
     *
     * @return <code>true</code> if the current JVM is at least Java 1.6
     * @see #getMajorJavaVersion()
     * @see #JAVA_6
     * @see #JAVA_7
     * @see #JAVA_8
     * @see #JAVA_9
     */
    public static boolean isAtLeastJava6() {
        return getMajorJavaVersion() >= JAVA_6;
    }

    /**
     * Convenience method to determine if the current JVM is at least Java 1.7 (Java 7).
     *
     * @return <code>true</code> if the current JVM is at least Java 1.7
     * @see #getMajorJavaVersion()
     * @see #JAVA_7
     * @see #JAVA_8
     * @see #JAVA_9
     */
    public static boolean isAtLeastJava7() {
        return getMajorJavaVersion() >= JAVA_7;
    }

    /**
     * Convenience method to determine if the current JVM is at least Java 1.8 (Java 8).
     *
     * @return <code>true</code> if the current JVM is at least Java 1.8
     * @see #getMajorJavaVersion()
     * @see #JAVA_8
     * @see #JAVA_9
     */
    public static boolean isAtLeastJava8() {
        return getMajorJavaVersion() >= JAVA_8;
    }

    /**
     * Convenience method to determine if the current JVM is at least Java 1.9 (Java 9).
     *
     * @return <code>true</code> if the current JVM is at least Java 1.9
     * @see #getMajorJavaVersion()
     * @see #JAVA_9
     */
    public static boolean isAtLeastJava9() {
        return getMajorJavaVersion() >= JAVA_9;
    }

}
