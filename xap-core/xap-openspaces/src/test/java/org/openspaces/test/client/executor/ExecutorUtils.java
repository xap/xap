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

/*
 * @(#)ExecutorUtils.java   Feb 5, 2007
 *
 * Copyright 2007 GigaSpaces Technologies Inc.
 */
package org.openspaces.test.client.executor;

import com.gigaspaces.start.SystemInfo;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.PrintStream;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.lang.reflect.Constructor;
import java.lang.reflect.Modifier;
import java.net.InetAddress;
import java.net.URL;
import java.net.UnknownHostException;
import java.rmi.Remote;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.StringTokenizer;

/**
 * This class provides helper methods.
 *
 * @author Igor Goldenberg
 * @since 1.0
 **/
public class ExecutorUtils {
    final static private Log _logger = LogFactory.getLog(ExecutorUtils.class);

    /**
     * Resolve to 1.5 by equality ; otherwise 1.4
     */
    public final static double JDK_VERSION = Double.parseDouble(System.getProperty("java.version").substring(0, 3));

    /**
     * avoid construction
     */
    private ExecutorUtils() {
    }

    /**
     * @return <code>true</code> if this JVM is running on Unix based system, otherwise Windows
     */
    public static boolean isUnixOS() {
        return File.separator.equals("/");
    }

    /**
     * Loads the requested resource and returns its input straem
     *
     * @param name name of resource to get
     * @return URL containing the resource
     */
    static public URL getResourceURL(String name) {
        URL result = null;

        //do not allow search using / prefix which does not work with classLoader.getResource()
        if (name.startsWith("/")) {
            name = name.substring(1);
        }
        Thread currentThread = Thread.currentThread();
        ClassLoader classLoader = currentThread.getContextClassLoader();
        result = classLoader.getResource(name);

        return result;
    }

    /**
     * Writes desired string data to appropriate file.
     *
     * @param context  String context.
     * @param fileName file name path.
     * @param append   if <code>true</code>, then bytes will be written to the end of the file
     *                 rather than the beginning
     * @throws IOException Failed to write data-context to file.
     */
    synchronized public static void toFile(String context, String fileName, boolean append)
            throws IOException {
        PrintStream ps = new PrintStream(new FileOutputStream(fileName, append));
        ps.println(context);
        ps.flush();
        ps.close();
    }

    /**
     * Read from supplied <b>text file</b> all lines and returns the String list.
     *
     * @param fileName the filename to read from.
     * @return list of file lines
     * @throws IOException Failed to open/read from supplied file.
     **/
    @SuppressWarnings("resource")
    synchronized public static List<String> getFileLines(String fileName)
            throws IOException {
        ArrayList<String> lineArray = new ArrayList<String>();

        FileReader fReader = null;

        try {
            fReader = new FileReader(fileName);
            BufferedReader reader = new BufferedReader(fReader);

            String line;
            while ((line = reader.readLine()) != null) {
                lineArray.add(line);
            }
        } finally {
            if (fReader != null)
                fReader.close();
        }

        return lineArray;
    }

    /**
     * Converts the stack trace of the specified exception to a string. NOTE: Pass the exception as
     * an argument to the external exception
     **/
    public static String getStackTrace(Throwable t) {
        StringWriter sw = new StringWriter();
        PrintWriter pw = new PrintWriter(sw);
        t.printStackTrace(pw);
        pw.flush();

        return sw.toString();
    }

    /**
     * Formats the duration as hh:mm:ss
     */
    public static String clockFormat(long duration) {
        long value = duration;
        value = value / 1000;
        long seconds = value % 60;
        value = value / 60;
        long minutes = value % 60;
        value = value / 60;
        long hours = value % 24;

        return ((hours < 10 ? "0" + hours : "" + hours) + ":"
                + (minutes < 10 ? "0" + minutes : "" + minutes) + ":"
                + (seconds < 10 ? "0" + seconds : "" + seconds));
    }

    /**
     * Format a time into a readable string
     */
    public static String timeFormat(long duration) {
        long value = duration;
        value = value / 1000;
        long seconds = value % 60;
        value = value / 60;
        long minutes = value % 60;
        value = value / 60;
        long hours = value % 24;
        long days = value / 24;

        String result = "";
        if (days > 0)
            result = days + (days > 1 ? " days" : " day") +
                    (hours > 0 ? ", " + hours + getHoursText(hours) : "") +
                    (minutes > 0 ? ", " + minutes + getMinutesText(minutes) : "") +
                    (seconds > 0 ? ", " + seconds + getSecondsText(seconds) : "");
        else if (hours > 0)
            result = hours + getHoursText(hours) +
                    (minutes > 0 ? ", " + minutes + getMinutesText(minutes) : "") +
                    (seconds > 0 ? ", " + seconds + getSecondsText(seconds) : "");
        else if (minutes > 0)
            result = minutes + getMinutesText(minutes) +
                    (seconds > 0 ? ", " + seconds + getSecondsText(seconds) : "");
        else if (seconds > 0)
            result = seconds + getSecondsText(seconds);
        else
            result = "0";
        return (result);
    }

    private static String getHoursText(long hours) {
        if (hours == 0)
            return (" hours");
        return ((hours > 1 ? " hours" : " hour"));
    }

    private static String getMinutesText(long minutes) {
        if (minutes == 0)
            return (" minutes");
        return ((minutes > 1 ? " minutes" : " minute"));
    }

    private static String getSecondsText(long seconds) {
        if (seconds == 0)
            return (" seconds");
        return ((seconds > 1 ? " seconds" : " second"));
    }

    /**
     * Tokenize the supplied string and return all tokens in List instance (allows duplication).
     *
     * @param str   a string to be parsed.
     * @param delim the delimiters or if <code>null</code> the default delimiter set is
     *              <code>"&nbsp;&#92;t&#92;n&#92;r&#92;f"</code>
     * @return list of parsed tokens.
     * @see #tokenize(String, String, boolean)
     **/
    public static List<String> tokenize(String str, String delim) {
        return tokenize(str, delim, true);
    }

    /**
     * Tokenize the supplied string and return all tokens in List instance.
     *
     * @param str        a string to be parsed.
     * @param delim      the delimiters or if <code>null</code> the default delimiter set is
     *                   <code>"&nbsp;&#92;t&#92;n&#92;r&#92;f"</code>
     * @param duplicates <code>true</code> if duplicates are allowed, <code>false</code> if
     *                   duplicates should be removed.
     * @return list of parsed tokens.
     */
    public static List<String> tokenize(String str, String delim, boolean duplicates) {
        ArrayList<String> listToken = new ArrayList<String>();

        if (str == null)
            return listToken;

        StringTokenizer st;
        if (delim != null)
            st = new StringTokenizer(str, delim);
        else
            st = new StringTokenizer(str);

        while (st.hasMoreTokens()) {
            String token = st.nextToken().trim();

            //are duplicates allowed?
            if (!duplicates && listToken.contains(token))
                continue;

            listToken.add(token);
        }

        return listToken;
    }


    /**
     * This is a very cool method which returns jar or directory from where the supplied class has
     * loaded.
     *
     * @param claz the class to get location.
     * @return jar/path location of supplied class.
     */
    @SuppressWarnings("rawtypes")
    public static String getClassLocation(Class claz) {
        return claz.getProtectionDomain().getCodeSource().getLocation().toString().substring(5);
    }

    /**
     * Get an anonymous port.
     *
     * @return An anonymous port created by instantiating a <code>java.net.ServerSocket</code> with
     * a port of 0.
     */
    public static int getAnonymousPort() {
        // default port
        int port = 45467;

        java.net.ServerSocket socket;
        try {
            socket = new java.net.ServerSocket(0);
            port = socket.getLocalPort();
            socket.close();
        } catch (IOException e) {
            if (_logger.isWarnEnabled())
                _logger.warn("Failed to get anonymous socket port.", e);
        }

        return port;
    }

    /**
     * wrap the supplied context with quotes only if Windows OS
     */
    public static String wrapQuotesIfWindowsOS(String context) {
        return ExecutorUtils.isUnixOS() ? context : ("\"" + context + "\"");
    }

    /**
     * Returns a list backed by the specified array.
     *
     * @param a the array by which the list will be backed.
     * @return a list view of the specified array.
     **/
    public static <T> List<T> asList(T... a) {
        ArrayList<T> list = new ArrayList<T>();
        for (T elem : a)
            list.add(elem);

        return list;
    }

    /**
     * Ensure that the class is valid, that is, that it has appropriate access: The class is public
     * and has public constructor with no-args.
     */
    @SuppressWarnings({"rawtypes", "unchecked"})
    public static void ensureValidClass(Class claz) {
        boolean ctorOK = false;

        try {
            // check if defined public constructor
            if (!Modifier.isPublic(claz.getModifiers())) {
                throw new IllegalArgumentException("Class [" + claz.getName() + "] not public");
            }

            Constructor ctor = claz.getConstructor(new Class[0]);
            ctorOK = Modifier.isPublic(ctor.getModifiers());
        } catch (NoSuchMethodException e) {
            ctorOK = false;
        } catch (SecurityException e) {
            ctorOK = false;
        }

        if (!ctorOK) {
            throw new IllegalArgumentException("Class [" + claz.getName() + "] needs public no-arg constructor");
        }
    }

    /**
     * Strip stack trace of provided {@link Throwable} object up to supplied classname <b>and</b>
     * method name. This method is useful when you wish to hide internal stackTrace.<br> This method
     * scans the {@link Throwable#getStackTrace()} and removes all {@link StackTraceElement} up to
     * supplied classname and methodName. The Throwable's cause is also scanned in a
     * <i>recursive</i> manner. <p> <u>NOTE</u>: If the provided classname and method was not found
     * in the stackTrace, the {@link Throwable} stackTrace stays unmodified.
     *
     * <pre>
     * For example:
     * Original stackTrace:
     * java.lang.Exception: Example stack trace...
     * at test.TestSTrace.applCall3(Test.java:926)
     * at test.TestSTrace.applCall2(Test.java:921)
     * at test.TestSTrace.applCall1(Test.java:916)
     * at test.TestSTrace.startApplCall(Test.java:912)
     * at test.TestSTrace.internalCall3(Test.java:941)
     * at test.TestSTrace.internalCall2(Test.java:936)
     * at test.TestSTrace.internalCall1(Test.java:931)
     * at test.TestSTrace.main(Test.java:977)
     *
     * TGridHelper.stripStackTrace(throwable, "test.TestSTrace", "startApplCall");
     * throwable.printStackTrace();
     *
     * After:
     * java.lang.Exception: Example stack trace...
     * at test.TestSTrace.applCall3(Test.java:941)
     * at test.TestSTrace.applCall2(Test.java:936)
     * at test.TestSTrace.applCall1(Test.java:931)
     * at test.TestSTrace.startApplCall(Test.java:912)
     * </pre>
     *
     * @param ex         the throwable object to strip stack trace.
     * @param className  strip stackTrace up to classname.
     * @param methodName strip stackTrace up to methodName.
     */
    public static void stripStackTrace(Throwable ex, String className, String methodName) {
        boolean found = false;
        StackTraceElement[] ste = ex.getStackTrace();
        List<StackTraceElement> stList = asList(ste);
        for (int i = stList.size() - 1; i >= 0; i--) {
            StackTraceElement traceList = stList.get(i);
            String traceClassName = traceList.getClassName();
            String traceMethodName = traceList.getMethodName();

            /* only if classname and method are equals with StackTraceElement */
            if (!(traceClassName.equals(className) && traceMethodName.equals(methodName)))
                stList.remove(i);
            else {
                found = true;
                break;
            }
        }

        /* replace only if provided classname and method was found the the stackTrace */
        if (found)
            ex.setStackTrace(stList.toArray(new StackTraceElement[stList.size()]));

        /* recursive call on all causes */
        Throwable cause = ex.getCause();
        if (cause != null)
            stripStackTrace(cause, className, methodName);
    }

    /**
     * Returns the package name of supplied class name. <p> Examples:
     * <pre>
     * TGridHelper.stripPackageName("java.lang.Object")   returns "java.lang"
     * TGridHelper.stripPackageName("Object") returns ""
     * </pre>
     *
     * @param className the class name.
     * @return the package name of the supplied class name, or empty string (if no preceding
     * package).
     */
    public static String stripPackageName(String className) {
        int inx = className.lastIndexOf(".");

        return inx == -1 ? "" : className.substring(0, inx);
    }

    /**
     * Omits the package name of the supplied class name. <p> Examples:
     * <pre>
     * TGridHelper.stripSimpleName("java.lang.Object")   returns "Object"
     * TGridHelper.stripSimpleName("Object") returns ""
     * </pre>
     *
     * @param className the class name.
     * @return the simple name of the supplied class name, or empty string (if no preceding
     * package).
     */
    public static String stripSimpleName(String className) {
        int inx = className.lastIndexOf(".") + 1;

        return inx == -1 ? "" : className.substring(inx);
    }

    /**
     * Returns <code>true</code> if this string is either null, an empty string, or it's {@link
     * String#trim()} is an empty string.
     *
     * @param s the string to check empty on.
     * @return <code>true</code> if null, "", or only white spaces. <code>false</code> otherwise.
     */
    public static boolean isEmptyString(String s) {
        if (s == null)
            return true;

        return "".equals(s.trim());
    }

    /**
     * Returns the host name of this machine, or "unknown host" if any exception was caught while
     * trying to retrieve it.
     *
     * @return The host name of this machine, or "unknown host"
     */
    public static String getHostName() {
        return SystemInfo.singleton().network().getHost().getHostName();
    }

    /**
     * Convenience method to return a Collection as a delimited (e.g. CSV) String. E.g. useful for
     * toString() implementations.
     *
     * @param coll   Collection to display
     * @param delim  delimiter to use (probably a ",")
     * @param prefix string to start each element with
     * @param suffix string to end each element with
     */
    public static String collectionToDelimitedString(Collection<String> coll, String delim, String prefix, String suffix) {
        if (coll == null) {
            return "";
        }

        StringBuilder sb = new StringBuilder();
        Iterator<String> it = coll.iterator();
        int i = 0;
        while (it.hasNext()) {
            if (i > 0) {
                sb.append(delim);
            }
            sb.append(prefix).append(it.next()).append(suffix);
            i++;
        }
        return sb.toString();
    }

    /**
     * Convenience method to return a Collection as a delimited (e.g. CSV) String. E.g. useful for
     * toString() implementations.
     *
     * @param coll  Collection to display
     * @param delim delimiter to use (probably a ",")
     */
    public static String collectionToDelimitedString(Collection<String> coll, String delim) {
        return collectionToDelimitedString(coll, delim, "", "");
    }

    /**
     * Replace all occurences of a substring within a string with another string.
     *
     * @param inString   String to examine
     * @param oldPattern String to replace
     * @param newPattern String to insert
     * @return a String with the replacements
     */
    public static String replace(String inString, String oldPattern, String newPattern) {
        if (inString == null) {
            return null;
        }
        if (oldPattern == null || newPattern == null) {
            return inString;
        }

        StringBuilder sbuf = new StringBuilder();
        // output StringBuffer we'll build up
        int pos = 0; // our position in the old string
        int index = inString.indexOf(oldPattern);
        // the index of an occurrence we've found, or -1
        int patLen = oldPattern.length();
        while (index >= 0) {
            sbuf.append(inString.substring(pos, index));
            sbuf.append(newPattern);
            pos = index + patLen;
            index = inString.indexOf(oldPattern, pos);
        }
        sbuf.append(inString.substring(pos));

        // remember to append any characters to the right of a match
        return sbuf.toString();
    }

    /**
     * Delete all occurrences of the given substring.
     *
     * @param pattern the pattern to delete all occurrences of
     */
    public static String delete(String inString, String pattern) {
        return replace(inString, pattern, "");
    }

    /**
     * Returns an array of all <code>java.rmi.Remote</code> interfaces implemented supplied class.
     *
     * @param claz The class to get all <code>java.rmi.Remote</code> interfaces.
     **/
    @SuppressWarnings("rawtypes")
    public static Class[] getRemoteDeclaredInterfaces(Class claz) {
        return getDeclaredInterfaces(claz, Remote.class);
    }

    /**
     * Returns an array of all declared interfaces of desired class (super-classes comes an
     * account). If <code>assignableClasses</code> is <code>null</code> all implemented interface
     * will be added to array, otherwise only <b>assignable</b> classes.
     *
     * @param claz The class to get all interfaces.
     **/
    @SuppressWarnings({"unchecked", "rawtypes"})
    public static Class[] getDeclaredInterfaces(Class claz, Class... assignableClasses) {
        ArrayList<Class> interfList = new ArrayList<Class>();

        while (!claz.equals(Object.class)) {
            for (Class cl : claz.getInterfaces()) {
            /* append only assignable interfaces */
                if (assignableClasses.length > 0) {
                    for (Class assignClaz : assignableClasses) {
                        if (assignClaz.isAssignableFrom(cl))
                            interfList.add(cl);
                    }
                } else
                    interfList.add(cl);
            }

            claz = claz.getSuperclass();
        }

        return interfList.toArray(new Class[interfList.size()]);
    }

}