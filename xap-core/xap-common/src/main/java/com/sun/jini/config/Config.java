/*
 * 
 * Copyright 2005 Sun Microsystems, Inc.
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 * 	http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * 
 */
package com.sun.jini.config;

import net.jini.config.Configuration;
import net.jini.config.ConfigurationException;
import net.jini.config.NoSuchEntryException;


import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Provides static methods for getting entries from a {@link Configuration}. This class cannot be
 * instantiated. <p> This class uses the {@link Logger} named <code>net.jini.config</code> to log
 * information at the following logging levels:
 *
 * <table border=1 cellpadding=5>
 *
 * <tr> <th> Level <th> Description
 *
 * <tr> <td> FINE <td> entries that are found, but do not meet the specified constraints (e.g. are
 * null)
 *
 * </table>
 *
 * @author Sun Microsystems, Inc.
 * @since 2.0
 */
@com.gigaspaces.api.InternalApi
public class Config {

    /**
     * This class cannot be instantiated.
     */
    private Config() {
        throw new AssertionError(
                "com.sun.jini.config.Config cannot be instantiated");
    }

    /**
     * Config logger.
     */
    static final Logger logger = LoggerFactory.getLogger("net.jini.config");

    /**
     * Obtains a non-<code>null</code> object from the specified {@link Configuration} using the
     * specified arguments. <p> Calling this method is equivalent to calling <code> {@link
     * #getNonNullEntry(Configuration, String, String, Class, Object, Object)
     * getNonNullEntry}(config, component, name, type, {@link Configuration#NO_DEFAULT}, {@link
     * Configuration#NO_DATA})</code>.
     *
     * @param config    the <code>Configuration</code> being consulted.
     * @param component the component being configured
     * @param name      the name of the entry for the component
     * @param type      the type of the object to be returned
     * @return a non-<code>null</code> object obtained from calling <code>config.getEntry</code>
     * using the other passed arguments
     * @throws NoSuchEntryException     if the underlying call to <code>config.getEntry</code> does
     * @throws ConfigurationException   if the underlying call to <code>config.getEntry</code> does,
     *                                  or if the returned entry is <code>null</code>. Any
     *                                  <code>Error</code> thrown while creating the object is
     *                                  propagated to the caller; it is not wrapped in a
     *                                  <code>ConfigurationException</code>
     * @throws IllegalArgumentException if the underlying call to <code>config.getEntry</code> does
     * @throws NullPointerException     if any of the arguments are <code>null</code>
     * @see Configuration#getEntry(String, String, Class)
     * @see #getNonNullEntry(Configuration, String, String, Class, Object, Object)
     */
    public static Object getNonNullEntry(Configuration config,
                                         String component, String name, Class type)
            throws ConfigurationException {
        return getNonNullEntry(config, component, name, type,
                Configuration.NO_DEFAULT, Configuration.NO_DATA);
    }

    /**
     * Obtains a non-<code>null</code> object from the specified {@link Configuration} using the
     * specified arguments. <p> Calling this method is equivalent to calling <code> {@link
     * #getNonNullEntry(Configuration, String, String, Class, Object, Object)
     * getNonNullEntry}(config, component, name, type, defaultValue, {@link
     * Configuration#NO_DATA})</code>.
     *
     * @param config       the <code>Configuration</code> being consulted.
     * @param component    the component being configured
     * @param name         the name of the entry for the component
     * @param type         the type of the object to be returned
     * @param defaultValue the object to return if no matching entry is found
     * @return a non-<code>null</code> object obtained from calling <code>config.getEntry</code>
     * using the other passed arguments
     * @throws NoSuchEntryException     if the underlying call to <code>config.getEntry</code> does
     * @throws ConfigurationException   if the underlying call to <code>config.getEntry</code> does,
     *                                  or if the returned entry is <code>null</code>. Any
     *                                  <code>Error</code> thrown while creating the object is
     *                                  propagated to the caller; it is not wrapped in a
     *                                  <code>ConfigurationException</code>
     * @throws IllegalArgumentException if the underlying call to <code>config.getEntry</code> does
     * @throws NullPointerException     if any of the arguments are <code>null</code>
     * @see Configuration#getEntry(String, String, Class, Object, Object)
     * @see #getNonNullEntry(Configuration, String, String, Class, Object, Object)
     */
    public static Object getNonNullEntry(Configuration config,
                                         String component, String name, Class type, Object defaultValue)
            throws ConfigurationException {
        return getNonNullEntry(config, component, name, type, defaultValue,
                Configuration.NO_DATA);
    }

    /**
     * Obtains a non-<code>null</code> object from the specified {@link Configuration} using the
     * specified arguments. <p> Calling this method is equivalent to calling <code>config.{@link
     * Configuration#getEntry(String, String, Class, Object, Object) getEntry}(component, name,
     * type, defaultValue, data)</code> after ensuring that <code>defaultValue</code> is
     * non-<code>null</code> and throwing <code>ConfigurationException</code> if the result is
     * <code>null</code>.
     *
     * @param config       the <code>Configuration</code> being consulted.
     * @param component    the component being configured
     * @param name         the name of the entry for the component
     * @param type         the type of the object to be returned
     * @param defaultValue the object to return if no matching entry is found
     * @param data         an object to use when computing the value of the entry, or
     *                     <code>Configuration#NO_DATA</code> to specify no data
     * @return a non-<code>null</code> object obtained from calling <code>config.getEntry</code>
     * using the other passed arguments
     * @throws NoSuchEntryException     if the underlying call to <code>config.getEntry</code> does
     * @throws ConfigurationException   if the underlying call to <code>config.getEntry</code> does,
     *                                  or if the returned entry is <code>null</code>. Any
     *                                  <code>Error</code> thrown while creating the object is
     *                                  propagated to the caller; it is not wrapped in a
     *                                  <code>ConfigurationException</code>
     * @throws IllegalArgumentException if the underlying call to <code>config.getEntry</code> does
     * @throws NullPointerException     if the <code>config</code>, <code>component</code>,
     *                                  <code>name</code>, <code>type</code>, or <code>defaultValue</code>
     *                                  arguments are <code>null</code>
     * @see Configuration#getEntry(String, String, Class, Object, Object)
     * @see #getNonNullEntry(Configuration, String, String, Class, Object, Object)
     */
    public static Object getNonNullEntry(Configuration config,
                                         String component, String name, Class type, Object defaultValue,
                                         Object data)
            throws ConfigurationException {
        if (defaultValue == null)
            throw new NullPointerException("defaultValue cannot be null");

        final Object result = config.getEntry(component, name, type,
                defaultValue, data);

        if (result == null) {
            if (logger.isDebugEnabled()) {
                logger.debug(
                        "{0}, component {1}, name {2}: cannot be null",
                        new Object[]{config, component, name});
            }

            throw new ConfigurationException("entry for component " +
                    component + ", name " + name + " cannot be null");
        }

        return result;
    }

    /**
     * Return <code>true</code> if the given number falls within the given range, inclusive.
     *
     * @param value the number to check
     * @param min   the low end of the range
     * @param max   the high end of the range
     * @return true if <code>min <= val <= max</code>
     */
    private static boolean inRange(int value, int min, int max) {
        return (min <= value) && (value <= max);
    }

    /**
     * Return <code>true</code> if the given number falls within the given range, inclusive.
     *
     * @param value the number to check
     * @param min   the low end of the range
     * @param max   the high end of the range
     * @return true if <code>min <= val <= max</code>
     */
    private static boolean inRange(long value, long min, long max) {
        return (min <= value) && (value <= max);
    }

    /**
     * Return <code>true</code> if the given number falls within the given range, inclusive.
     *
     * @param value the number to check
     * @param min   the low end of the range
     * @param max   the high end of the range
     * @return true if <code>min <= val <= max</code>
     */
    private static boolean inRange(float value, float min, float max) {
        return (min <= value) && (value <= max);
    }

    /**
     * Obtains a <code>long</code> that falls within the given inclusive range from the specified
     * {@link Configuration} using the specified component and entry names.
     *
     * @param config       the <code>Configuration</code> being consulted.
     * @param component    the component being configured
     * @param name         the name of the entry for the component
     * @param defaultValue the <code>long</code> to return if no matching entry is found
     * @param min          the minimum value the entry should have
     * @param max          the maximum value the entry should have
     * @return a long obtained from calling <code>config.getEntry</code> using
     * <code>component</code>, <code>name</code>, and <code>defaultValue</code>.
     * @throws NoSuchEntryException     if the underlying call to <code>config.getEntry</code> does
     * @throws ConfigurationException   if the underlying call to <code>config.getEntry</code> does,
     *                                  or if the returned entry is <code>null</code>, or if
     *                                  returned value is not between <code>min</code> and
     *                                  <code>max</code>, inclusive. Any <code>Error</code> thrown
     *                                  while creating the object is propagated to the caller; it is
     *                                  not wrapped in a <code>ConfigurationException</code>
     * @throws IllegalArgumentException if the underlying call to <code>config.getEntry</code> does,
     *                                  if <code>defaultValue</code> is not between <code>min</code>
     *                                  and <code>max</code> inclusive, or if <code>min</code> is
     *                                  larger than <code>max</code>
     * @throws NullPointerException     if <code>config</code>, <code>component</code> or
     *                                  <code>name</code> is <code>null</code>.
     * @see Configuration#getEntry(String, String, Class, Object)
     */
    public static long getLongEntry(Configuration config,
                                    String component, String name, long defaultValue,
                                    long min, long max)
            throws ConfigurationException {
        if (min > max)
            throw new IllegalArgumentException(
                    "min must be less than or equal to max");

        if (!inRange(defaultValue, min, max))
            throw new IllegalArgumentException("defaultValue (" +
                    defaultValue + ") must be between " + min + " and " + max);

        final long rslt = ((Long) config.getEntry(component, name, long.class,
                new Long(defaultValue))).longValue();

        if (!inRange(rslt, min, max)) {
            if (logger.isDebugEnabled()) {
                logger.debug(
                        "{0}, component {1}, name {2}: entry is out of range, " +
                                "value: {3}, valid range: {4}:{5}",
                        new Object[]{config, component, name, new Long(rslt),
                                new Long(min), new Long(max)});
            }

            throw new ConfigurationException("entry for component " +
                    component + ", name " + name + " must be between " +
                    min + " and " + max + ", has a value of " + rslt);
        }

        return rslt;
    }

    /**
     * Obtains an <code>int</code> that falls within the given inclusive range from the specified
     * {@link Configuration} using the specified component and entry names.
     *
     * @param config       the <code>Configuration</code> being consulted.
     * @param component    the component being configured
     * @param name         the name of the entry for the component
     * @param defaultValue the <code>int</code> to return if no matching entry is found
     * @param min          the minimum value the entry should have
     * @param max          the maximum value the entry should have
     * @return a int obtained from calling <code>config.getEntry</code> using
     * <code>component</code>, <code>name</code>, and <code>defaultValue</code>.
     * @throws NoSuchEntryException     if the underlying call to <code>config.getEntry</code> does
     * @throws ConfigurationException   if the underlying call to <code>config.getEntry</code> does,
     *                                  or if the returned entry is <code>null</code>, or if
     *                                  returned value is not between <code>min</code> and
     *                                  <code>max</code>, inclusive. Any <code>Error</code> thrown
     *                                  while creating the object is propagated to the caller; it is
     *                                  not wrapped in a <code>ConfigurationException</code>
     * @throws IllegalArgumentException if the underlying call to <code>config.getEntry</code> does,
     *                                  if <code>defaultValue</code> is not between <code>min</code>
     *                                  and <code>max</code> inclusive, or if <code>min</code> is
     *                                  larger than <code>max</code>
     * @throws NullPointerException     if <code>config</code>, <code>component</code> or
     *                                  <code>name</code> is <code>null</code>.
     * @see Configuration#getEntry(String, String, Class, Object)
     */
    public static int getIntEntry(Configuration config,
                                  String component, String name, int defaultValue,
                                  int min, int max)
            throws ConfigurationException {
        if (min > max)
            throw new IllegalArgumentException(
                    "min must be less than or equal to max");

        if (!inRange(defaultValue, min, max))
            throw new IllegalArgumentException("defaultValue (" +
                    defaultValue + ") must be between " + min + " and " + max);

        final int rslt = ((Integer) config.getEntry(component, name, int.class,
                new Integer(defaultValue))).intValue();

        if (!inRange(rslt, min, max)) {
            if (logger.isDebugEnabled()) {
                logger.debug(
                        "{0}, component {1}, name {2}: entry is out of range, " +
                                "value: {3}, valid range: {4}:{5}",
                        new Object[]{config, component, name, new Integer(rslt),
                                new Integer(min), new Integer(max)});
            }

            throw new ConfigurationException("entry for component " +
                    component + ", name " + name + " must be between " +
                    min + " and " + max + ", has a value of " + rslt);
        }

        return rslt;
    }

    /**
     * Obtains a <code>float</code> that falls within the given inclusive range from the specified
     * {@link Configuration} using the specified component and entry names.
     *
     * @param config       the <code>Configuration</code> being consulted.
     * @param component    the component being configured
     * @param name         the name of the entry for the component
     * @param defaultValue the <code>float</code> to return if no matching entry is found
     * @param min          the minimum value the entry should have
     * @param max          the maximum value the entry should have
     * @return a float obtained from calling <code>config.getEntry</code> using
     * <code>component</code>, <code>name</code>, and <code>defaultValue</code>.
     * @throws NoSuchEntryException     if the underlying call to <code>config.getEntry</code> does
     * @throws ConfigurationException   if the underlying call to <code>config.getEntry</code> does,
     *                                  or if the returned entry is <code>null</code>, or if
     *                                  returned value is not between <code>min</code> and
     *                                  <code>max</code>, inclusive. Any <code>Error</code> thrown
     *                                  while creating the object is propagated to the caller; it is
     *                                  not wrapped in a <code>ConfigurationException</code>
     * @throws IllegalArgumentException if the underlying call to <code>config.getEntry</code> does,
     *                                  if <code>defaultValue</code> is not between <code>min</code>
     *                                  and <code>max</code> inclusive, or if <code>min</code> is
     *                                  larger than <code>max</code>
     * @throws NullPointerException     if <code>config</code>, <code>component</code> or
     *                                  <code>name</code> is <code>null</code>.
     * @see Configuration#getEntry(String, String, Class, Object)
     */
    public static float getFloatEntry(Configuration config,
                                      String component, String name, float defaultValue,
                                      float min, float max)
            throws ConfigurationException {
        if (min > max)
            throw new IllegalArgumentException(
                    "min must be less than or equal to max");

        if (!inRange(defaultValue, min, max))
            throw new IllegalArgumentException("defaultValue (" +
                    defaultValue + ") must be between " + min + " and " + max);

        final float rslt = ((Float) config.getEntry(component, name,
                float.class, new Float(defaultValue))).floatValue();

        if (!inRange(rslt, min, max)) {
            if (logger.isDebugEnabled()) {
                logger.debug(
                        "{0}, component {1}, name {2}: entry is out of range, " +
                                "value: {3}, valid range: {4}:{5}",
                        new Object[]{config, component, name, new Float(rslt),
                                new Float(min), new Float(max)});
            }

            throw new ConfigurationException("entry for component " +
                    component + ", name " + name + " must be between " +
                    min + " and " + max + ", has a value of " + rslt);
        }

        return rslt;
    }
}
