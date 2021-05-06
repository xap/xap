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

import com.gigaspaces.internal.io.BootIOUtils;

import java.util.*;
import java.util.concurrent.TimeUnit;

/**
 * Miscellaneous string utility methods. Mainly for internal use within the framework; consider
 * Jakarta's Commons Lang for a more comprehensive suite of string utilities.
 *
 * <p>This class delivers some simple functionality that should really be provided by the core Java
 * String and StringBuffer classes, such as the ability to replace all occurrences of a given
 * substring in a target string. It also provides easy-to-use methods to convert between delimited
 * strings, such as CSV strings, and collections and arrays.
 *
 * @author kimchy
 */
public abstract class StringUtils {

    public static final String FOLDER_SEPARATOR = "/";

    public static final String NEW_LINE = System.getProperty("line.separator");

    public static String FORMAT_TIME_STAMP = "%1$tF %1$tT.%1$tL";

    //---------------------------------------------------------------------
    // General convenience methods for working with Strings
    //---------------------------------------------------------------------

    public static boolean isEmpty(String str) {
        return str == null || str.length() == 0;
    }

    /**
     * Check if a String has length.
     * <p><pre>
     * StringUtils.hasLength(null) = false
     * StringUtils.hasLength("") = false
     * StringUtils.hasLength(" ") = true
     * StringUtils.hasLength("Hello") = true
     * </pre>
     *
     * @param str the String to check, may be <code>null</code>
     * @return <code>true</code> if the String is not null and has length
     */
    public static boolean hasLength(String str) {
        return (str != null && str.length() > 0);
    }

    /**
     * Check if a String has text. More specifically, returns <code>true</code> if the string not
     * <code>null<code>, it's <code>length is > 0</code>, and it has at least one non-whitespace
     * character.
     * <p><pre>
     * StringUtils.hasText(null) = false
     * StringUtils.hasText("") = false
     * StringUtils.hasText(" ") = false
     * StringUtils.hasText("12345") = true
     * StringUtils.hasText(" 12345 ") = true
     * </pre>
     *
     * @param str the String to check, may be <code>null</code>
     * @return <code>true</code> if the String is not null, length > 0, and not whitespace only
     * @see java.lang.Character#isWhitespace
     */
    public static boolean hasText(String str) {
        int strLen;
        if (str == null || (strLen = str.length()) == 0) {
            return false;
        }
        for (int i = 0; i < strLen; i++) {
            if (!Character.isWhitespace(str.charAt(i))) {
                return true;
            }
        }
        return false;
    }

    public static boolean equalsIgnoreCase(String str1, String str2) {
        if (str1 == null)
            return str2 == null;
        return str1.equalsIgnoreCase(str2);
    }

    /**
     * Test if the given String starts with the specified prefix, ignoring upper/lower case.
     *
     * @param str    the String to check
     * @param prefix the prefix to look for
     * @see java.lang.String#startsWith
     */
    public static boolean startsWithIgnoreCase(String str, String prefix) {
        return str != null && prefix != null &&
                str.regionMatches(true, 0, prefix, 0, prefix.length());
    }

    /**
     * Test if the given String ends with the specified suffix, ignoring upper/lower case.
     *
     * @param str    the String to check
     * @param suffix the suffix to look for
     * @see java.lang.String#endsWith
     */
    public static boolean endsWithIgnoreCase(String str, String suffix) {
        return str != null && suffix != null &&
                str.regionMatches(true, str.length() - suffix.length(), suffix, 0, suffix.length());
    }

    public static int indexOfIgnoreCase(String str1, String str2) {
        final int length = str2.length();
        final int lastPos = str1.length() - length;
        for (int pos = 0; pos <= lastPos; pos++)
            if (str1.regionMatches(true, pos, str2, 0, length))
                return pos;
        return -1;
    }

    /**
     * Count the occurrences of the character c in string s.
     *
     * @param s String to scan.
     * @param c Character to find and count.
     * @return Number of occurrences of c in s.
     */
    public static int countOccurrencesOf(String s, char c) {
        if (s == null || s.length() == 0)
            return 0;

        char[] chars = s.toCharArray();
        int count = 0;
        for (char aChar : chars)
            if (aChar == c)
                count++;
        return count;
    }

    //---------------------------------------------------------------------
    // Convenience methods for working with String arrays
    //---------------------------------------------------------------------

    /**
     * Copy the given Collection into a String array. The Collection must contain String elements
     * only.
     *
     * @param collection the Collection to copy
     * @return the String array (<code>null</code> if the Collection was <code>null</code> as well)
     */
    public static String[] toStringArray(Collection<String> collection) {
        if (collection == null) {
            return null;
        }
        return collection.toArray(new String[collection.size()]);
    }

    public static String join(String[] array, String delimiter, int firstIndex, int count) {
        switch (count) {
            case 0:
                return "";
            case 1:
                return array[firstIndex];
            case 2:
                return array[firstIndex] +
                        delimiter + array[firstIndex + 1];
            case 3:
                return array[firstIndex] +
                        delimiter + array[firstIndex + 1] +
                        delimiter + array[firstIndex + 2];
            case 4:
                return array[firstIndex] +
                        delimiter + array[firstIndex + 1] +
                        delimiter + array[firstIndex + 2] +
                        delimiter + array[firstIndex + 3];
            case 5:
                return array[firstIndex] +
                        delimiter + array[firstIndex + 1] +
                        delimiter + array[firstIndex + 2] +
                        delimiter + array[firstIndex + 3] +
                        delimiter + array[firstIndex + 4];
            default:
                StringBuilder sb = new StringBuilder(array[0]);
                for (int i = firstIndex + 1; i < count; i++) {
                    sb.append(delimiter);
                    sb.append(array[i]);
                }

                return sb.toString();
        }
    }

    public static String join(Collection<String> collection, String separator) {
        if (collection == null)
            return null;
        if (collection.isEmpty())
            return "";
        StringBuilder sb = new StringBuilder();
        for (String s : collection) {
            if (sb.length() != 0)
                sb.append(separator);
            sb.append(s);
        }
        return sb.toString();
    }

    public static List<String> toList(String s, String delimiter) {
        if (s == null || s.length() == 0)
            return Collections.emptyList();
        final List<String> result = new ArrayList<String>();
        final StringTokenizer tokenizer = new StringTokenizer(s, delimiter);
        while (tokenizer.hasMoreTokens())
            result.add(tokenizer.nextToken());
        return result;
    }

    public static Properties toProperties(String s, String propertyDelimiter, String keyValueSeparator) {
        Properties properties = new Properties();
        List<String> tokens = StringUtils.toList(s, propertyDelimiter);
        for (String token : tokens) {
            int separatorPos = token.indexOf(keyValueSeparator);
            String key = token.substring(0, separatorPos);
            String value = token.substring(separatorPos + 1);
            properties.setProperty(key, value);
        }
        return properties;
    }

    /**
     * Tokenize the given String into a String array via a StringTokenizer. Trims tokens and omits
     * empty tokens. <p>The given delimiters string is supposed to consist of any number of
     * delimiter characters. Each of those characters can be used to separate tokens. A delimiter is
     * always a single character; for multi-character delimiters, consider using
     * <code>delimitedListToStringArray</code>
     *
     * @param str        the String to tokenize
     * @param delimiters the delimiter characters, assembled as String (each of those characters is
     *                   individually considered as delimiter).
     * @return an array of the tokens
     * @see java.util.StringTokenizer
     * @see java.lang.String#trim
     * @see #delimitedListToStringArray
     */
    public static String[] tokenizeToStringArray(String str, String delimiters) {
        return tokenizeToStringArray(str, delimiters, true, true);
    }

    /**
     * Tokenize the given String into a String array via a StringTokenizer. <p>The given delimiters
     * string is supposed to consist of any number of delimiter characters. Each of those characters
     * can be used to separate tokens. A delimiter is always a single character; for multi-character
     * delimiters, consider using <code>delimitedListToStringArray</code>
     *
     * @param str               the String to tokenize
     * @param delimiters        the delimiter characters, assembled as String (each of those
     *                          characters is individually considered as delimiter)
     * @param trimTokens        trim the tokens via String's <code>trim</code>
     * @param ignoreEmptyTokens omit empty tokens from the result array (only applies to tokens that
     *                          are empty after trimming; StringTokenizer will not consider
     *                          subsequent delimiters as token in the first place).
     * @return an array of the tokens
     * @see java.util.StringTokenizer
     * @see java.lang.String#trim
     * @see #delimitedListToStringArray
     */
    public static String[] tokenizeToStringArray(
            String str, String delimiters, boolean trimTokens, boolean ignoreEmptyTokens) {

        StringTokenizer st = new StringTokenizer(str, delimiters);
        List<String> tokens = new ArrayList<String>();
        while (st.hasMoreTokens()) {
            String token = st.nextToken();
            if (trimTokens) {
                token = token.trim();
            }
            if (!ignoreEmptyTokens || token.length() > 0) {
                tokens.add(token);
            }
        }
        return toStringArray(tokens);
    }

    /**
     * Take a String which is a delimited list and convert it to a String array. <p>A single
     * delimiter can consists of more than one character: It will still be considered as single
     * delimiter string, rather than as bunch of potential delimiter characters - in contrast to
     * <code>tokenizeToStringArray</code>.
     *
     * @param str       the input String
     * @param delimiter the delimiter between elements (this is a single delimiter, rather than a
     *                  bunch individual delimiter characters)
     * @return an array of the tokens in the list
     * @see #tokenizeToStringArray
     */
    public static String[] delimitedListToStringArray(String str, String delimiter) {
        if (str == null) {
            return new String[0];
        }
        if (delimiter == null) {
            return new String[]{str};
        }

        List<String> result = new ArrayList<String>();
        if ("".equals(delimiter)) {
            for (int i = 0; i < str.length(); i++) {
                result.add(str.substring(i, i + 1));
            }
        } else {
            int pos = 0;
            int delPos;
            while ((delPos = str.indexOf(delimiter, pos)) != -1) {
                result.add(str.substring(pos, delPos));
                pos = delPos + delimiter.length();
            }
            if (str.length() > 0 && pos <= str.length()) {
                // Add rest of String, but not in case of empty input.
                result.add(str.substring(pos));
            }
        }
        return toStringArray(result);
    }

    /**
     * Convert a CSV list into an array of Strings.
     *
     * @param str CSV list
     * @return an array of Strings, or the empty array if s is null
     */
    public static String[] commaDelimitedListToStringArray(String str) {
        return delimitedListToStringArray(str, ",");
    }

    /**
     * Convenience method to return a String array as a delimited (e.g. CSV) String. E.g. useful for
     * toString() implementations.
     *
     * @param arr   array to display. Elements may be of any type (toString will be called on each
     *              element).
     * @param delim delimiter to use (probably a ",")
     */
    public static String arrayToDelimitedString(Object[] arr, String delim) {
        if (arr == null) {
            return "";
        }

        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < arr.length; i++) {
            if (i > 0) {
                sb.append(delim);
            }
            sb.append(arr[i]);
        }
        return sb.toString();
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
     * Convenience method to return a String array as a CSV String. E.g. useful for toString()
     * implementations.
     *
     * @param arr array to display. Elements may be of any type (toString will be called on each
     *            element).
     */
    public static String arrayToCommaDelimitedString(Object[] arr) {
        return arrayToDelimitedString(arr, ",");
    }

    /**
     * Convenience method to return a Collection as a CSV String. E.g. useful for toString()
     * implementations.
     *
     * @param coll Collection to display
     */
    public static String collectionToCommaDelimitedString(Collection<String> coll) {
        return collectionToDelimitedString(coll, ",");
    }

    /**
     * Transforms array of String objects to Set
     *
     * @param array array of String instances
     * @return set
     */
    public static Set<String> convertArrayToSet(String[] array) {
        Set<String> resultSet = new HashSet<String>(array.length);
        Collections.addAll(resultSet, array);
        return resultSet;
    }

    /**
     * Transforms string with elements separated by "," to Set of elements
     *
     * @param locatorsStr elements delimetered by ","
     * @return set
     */
    public static Set<String> getParametersSet(String locatorsStr) {
        if (isEmpty(locatorsStr, true)) {
            return new HashSet<String>(1);
        }

        String[] locatorsArray = tokenizeToStringArray(locatorsStr, ",");
        return convertArrayToSet(locatorsArray);
    }

    private static boolean isEmpty(String s, boolean isWithTrim) {
        if (s == null || s.length() == 0)
            return true;

        if (isWithTrim)
            s = s.trim();

        return s.length() == 0;
    }

    public static void appendProperties(StringBuilder sb, Properties properties) {
        if (properties == null)
            sb.append("null");
        else {
            for (Enumeration<?> e = properties.propertyNames(); e.hasMoreElements(); ) {
                String propName = (String) e.nextElement();
                sb.append("\n\t XPath element key: ").append(propName);
                String propValue = properties.getProperty(propName);
                sb.append("\n\t Value: ").append(propValue);
                sb.append('\n');
            }
        }
    }

    public static String[] convertKeyValuePairsToArray(Map<String, String> value, String keyValueSeperator) {
        final List<String> keyValuePairs = new ArrayList<String>();
        for (final String pairkey : value.keySet()) {
            final String pairvalue = value.get(pairkey);
            if (pairkey.contains(keyValueSeperator)) {
                throw new IllegalArgumentException("Key " + pairkey + " cannot contain separator " + keyValueSeperator);
            }
            keyValuePairs.add(pairkey + keyValueSeperator + pairvalue);
        }
        return keyValuePairs.toArray(new String[keyValuePairs.size()]);
    }

    public static Map<String, String> convertArrayToKeyValuePairs(String[] pairs, String keyValueSeperator) {
        Map<String, String> value = new HashMap<String, String>();
        for (String pair : pairs) {
            int sepindex = pair.indexOf(keyValueSeperator);
            String pairkey = pair.substring(0, sepindex);
            String pairvalue = pair.substring(sepindex + 1);
            value.put(pairkey, pairvalue);
        }
        return value;
    }

    public static String getTimeStamp() {
        return getTimeStamp(System.currentTimeMillis());
    }

    public static String getTimeStamp(long timeInMillis) {
        return String.format(FORMAT_TIME_STAMP, timeInMillis);
    }

    public static String getSuffix(String s, String separator) {
        if (!hasLength(s))
            return s;

        int lastIndexOf = s.lastIndexOf(separator);
        if (lastIndexOf == -1)
            return s;

        return s.substring(lastIndexOf + separator.length(), s.length());
    }

    public static String getFileName(String path, boolean removeExtension) {
        String name = getSuffix(path, "/");
        if (name == path) {
            name = getSuffix(path, "\\");
        }
        if (removeExtension) {
            int pos = name.lastIndexOf(".");
            if (pos != -1)
                name = name.substring(0, pos);
        }
        return name;
    }

    public static String getCurrentStackTrace() {
        StackTraceElement[] stackTrace = Thread.currentThread().getStackTrace();
        StringBuilder sb = new StringBuilder();
        //Skip first two stack elements since they are from this method
        for (int i = 2; i < stackTrace.length; ++i)
            sb.append("\tat ").append(stackTrace[i].toString()).append("\n");
        return sb.toString();
    }

    public static boolean isStrictPrefix(String s, String prefix) {
        return s.startsWith(prefix) && s.length() > prefix.length();
    }

    public static String trimToNull(String s) {
        if (s == null)
            return null;
        String trimmed = s.trim();
        return trimmed.length() != 0 ? trimmed : null;
    }

    public static void resolvePlaceholders(Properties properties) {
        resolvePlaceholders(properties, System.getenv());
    }

    public static void resolvePlaceholders(Properties properties, Map<String, String> environment) {
        for (Map.Entry entry : properties.entrySet()) {
            String key = (String) entry.getKey();
            String value = (String) entry.getValue();
            properties.put(key, resolvePlaceholders(value, environment));
        }
    }

    public static String resolvePlaceholders(String text) {
        return resolvePlaceholders(text, System.getenv());
    }

    public static String resolvePlaceholders(String text, Map<String, String> environment) {
        return resolvePlaceholders(text, environment, "${", "}");
    }

    public static String resolvePlaceholders(String text, Map<String, String> environment, String prefix, String suffix) {
        if (text == null) {
            return null;
        }

        StringBuffer buf = new StringBuffer(text);

        // The following code does not use JDK 1.4's StringBuffer.indexOf(String)
        // method to retain JDK 1.3 compatibility. The slight loss in performance
        // is not really relevant, as this code will typically just run on startup.

        int startIndex = text.indexOf(prefix);
        while (startIndex != -1) {
            int endIndex = buf.toString().indexOf(suffix, startIndex + prefix.length());
            if (endIndex != -1) {
                String placeholder = buf.substring(startIndex + prefix.length(), endIndex);
                int nextIndex = endIndex + suffix.length();
                try {
                    String propVal = System.getProperty(placeholder);
                    if (propVal == null) {
                        // Fall back to searching the system environment.
                        propVal = environment.get(placeholder);
                    }
                    if (propVal != null) {
                        buf.replace(startIndex, endIndex + suffix.length(), propVal);
                        nextIndex = startIndex + propVal.length();
                    } else {
                        throw new IllegalArgumentException("Could not resolve placeholder '" + placeholder + "' in [" + text +
                                "] as system property: neither system property nor environment variable found");
                    }
                } catch (Throwable ex) {
                    throw new IllegalArgumentException("Could not resolve placeholder '" + placeholder + "' in [" + text +
                            "] as system property", ex);
                }
                startIndex = buf.toString().indexOf(prefix, nextIndex);
            } else {
                startIndex = -1;
            }
        }

        return buf.toString();
    }

    public static String extract(String s, String prefix, String suffix) {
        final int beginIndex = s.indexOf(prefix);
        if (beginIndex == -1)
            return null;
        final int endIndex = s.indexOf(suffix, beginIndex + prefix.length());
        if (endIndex == -1)
            return null;
        return s.substring(beginIndex + prefix.length(), endIndex);
    }

    public static Long parseStringAsBytes(String property) {
        return BootIOUtils.parseStringAsBytes(property);
    }

    public static Long parseDurationAsMillis(String property) {
        if (property == null || property.length() == 0)
            return null;

        // Find first non-digit char:
        int pos = 0;
        while (pos < property.length() && Character.isDigit(property.charAt(pos)))
            pos++;

        String prefix = property.substring(0, pos);
        long number = Long.parseLong(prefix);
        String suffix = pos < property.length() ? property.substring(pos) : null;
        TimeUnit timeUnit = parseTimeUnit(suffix, TimeUnit.MILLISECONDS);
        return timeUnit.toMillis(number);
    }

    public static TimeUnit parseTimeUnit(String s, TimeUnit defaultValue) {
        if (s == null) return defaultValue;
        if (s.equalsIgnoreCase("n")) return TimeUnit.NANOSECONDS;
        if (s.equalsIgnoreCase("u")) return TimeUnit.MICROSECONDS;
        if (s.equalsIgnoreCase("ms")) return TimeUnit.MILLISECONDS;
        if (s.equalsIgnoreCase("s")) return TimeUnit.SECONDS;
        if (s.equalsIgnoreCase("m")) return TimeUnit.MINUTES;
        if (s.equalsIgnoreCase("h")) return TimeUnit.HOURS;
        if (s.equalsIgnoreCase("d")) return TimeUnit.DAYS;
        throw new IllegalArgumentException("Invalid time unit: '" + s + "'");
    }
}
