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

package net.jini.config;

import com.gigaspaces.logger.LogUtils;

import java.util.Arrays;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * A skeletal implementation of the <code>Configuration</code> interface, used to simplify writing
 * implementations. This class checks the validity of arguments to the <code>getEntry</code>
 * methods, checks that the result matches the requested type, and wraps exceptions other than
 * {@link Error} or {@link ConfigurationException} in a <code>ConfigurationException</code>.
 * Subclasses need to implement the {@link #getEntryInternal(String, String, Class, Object)
 * getEntryInternal} method, which supplies entry values, throws {@link NoSuchEntryException} if no
 * matching entry is found, and performs any desired primitive conversions. The
 * <code>getEntryInternal</code> method should return primitive values as instances of {@link
 * Primitive}.
 *
 * @author Sun Microsystems, Inc.
 * @since 2.0
 *
 *
 * This implementation uses the {@link Logger} named <code>net.jini.config</code> to log information
 * at the following logging levels: <p>
 *
 * <table border="1" cellpadding="5" summary="Describes logging performed by the
 * AbstractConfiguration class at different logging levels">
 *
 * <caption halign="center" valign="top"><b><code> net.jini.config</code></b></caption>
 *
 * <tr> <th scope="col"> Level <th scope="col"> Description
 *
 * <tr> <td> {@link Level#FINE FINE} <td> problems getting entries, including getting entries
 * that are not found
 *
 * <tr> <td> {@link Level#FINE FINE} <td> returning default values
 *
 * <tr> <td> {@link Level#FINER FINER} <td> getting existing entries
 *
 * </table>
 */
public abstract class AbstractConfiguration implements Configuration {

    /**
     * A sorted array of names that cannot be used for identifiers.  This list includes the names of
     * all Java programming language keywords, plus 'null', 'true', and 'false', which are not
     * keywords, but are not permitted as identifiers.
     */
    private static final String[] reservedNames = {
            "abstract", "assert", "boolean", "break", "byte", "case", "catch",
            "char", "class", "const", "continue", "default", "do", "double",
            "else", "extends", "false", "final", "finally", "float", "for", "goto",
            "if", "implements", "import", "instanceof", "int", "interface", "long",
            "native", "new", "null", "package", "private", "protected", "public",
            "return", "short", "static", "strictfp", "super", "switch",
            "synchronized", "this", "throw", "throws", "transient", "true", "try",
            "void", "volatile", "while"
    };

    /**
     * Config logger.
     */
    static final Logger logger = Logger.getLogger("net.jini.config");

    /**
     * Represents the value of an entry with a primitive type. Subclasses of {@link
     * AbstractConfiguration} that contain primitive entries should return instances of this class
     * from their {@link #getEntryInternal getEntryInternal} methods.
     *
     * @since 2.0
     */
    public static final class Primitive {

        /**
         * The value, as a wrapper instance.
         */
        private final Object value;

        /**
         * The primitive type.
         */
        private final Class type;

        /**
         * Creates an object that represents a primitive value of the type associated with the
         * specified primitive wrapper object.
         *
         * @param value the primitive wrapper object
         * @throws IllegalArgumentException if <code>value</code> is not an instance of a primitive
         *                                  wrapper class
         */
        public Primitive(Object value) {
            this.value = value;
            type = (value != null)
                    ? Utilities.getPrimitiveType(value.getClass()) : null;
            if (type == null) {
                throw new IllegalArgumentException(
                        "value is not a primitive: " + value);
            }
        }

        /**
         * Returns the primitive value associated with this object, represented as a primitive
         * wrapper instance.
         *
         * @return the value of this object, as a primitive wrapper instance
         */
        public Object getValue() {
            return value;
        }

        /**
         * Returns the primitive type of the value associated with this object.
         *
         * @return the primitive type of the value associated with this object
         */
        public Class getType() {
            return type;
        }

        /**
         * Returns a string representation of this object.
         */
        public String toString() {
            return "Primitive[(" + type + ") " + value + "]";
        }

        /**
         * Returns <code>true</code> if the argument is a <code>Primitive</code> for which the
         * result of calling <code>getValue</code> is the same as the value for this instance,
         * otherwise <code>false</code>.
         */
        public boolean equals(Object obj) {
            return obj instanceof Primitive &&
                    value.equals(((Primitive) obj).value);
        }

        /**
         * Returns a hash code value for this object.
         */
        public int hashCode() {
            return value.hashCode();
        }
    }

    /**
     * Creates an instance of this class.
     */
    protected AbstractConfiguration() {
    }

    /**
     * Returns an object of the specified type created using the information in the entry matching
     * the specified component and name, which must be found, and supplying no data. If
     * <code>type</code> is a primitive type, then the result is returned as an instance of the
     * associated wrapper class. Repeated calls with the same arguments may or may not return the
     * identical object. <p>
     *
     * The default implementation checks that <code>component</code>, <code>name</code>, and
     * <code>type</code> are not <code>null</code>; that <code>component</code> is a valid qualified
     * identifier; and that <code>name</code> is a valid identifier. It returns the result of
     * calling {@link #getEntryInternal(String, String, Class, Object) getEntryInternal} with the
     * specified arguments, as well as {@link #NO_DEFAULT} and {@link #NO_DATA}, converting results
     * of type {@link Primitive} into the associated wrapper type. If the call throws an exception
     * other than an {@link Error} or a {@link ConfigurationException}, it throws a
     * <code>ConfigurationException</code> with the original exception as the cause.
     *
     * @throws NoSuchEntryException     {@inheritDoc}
     * @throws IllegalArgumentException {@inheritDoc}
     * @throws NullPointerException     {@inheritDoc}
     */
    public Object getEntry(String component, String name, Class type)
            throws ConfigurationException {
        return getEntryInternal(component, name, type, NO_DEFAULT, NO_DATA);
    }

    /**
     * Returns an object of the specified type created using the information in the entry matching
     * the specified component and name, and supplying no data, returning the default value if no
     * matching entry is found and the default value is not {@link #NO_DEFAULT}. If
     * <code>type</code> is a primitive type, then the result is returned as an instance of the
     * associated wrapper class. Repeated calls with the same arguments may or may not return the
     * identical object. <p>
     *
     * The default implementation checks that <code>component</code>, <code>name</code>, and
     * <code>type</code> are not <code>null</code>; that <code>component</code> is a valid qualified
     * identifier; that <code>name</code> is a valid identifier; and that <code>defaultValue</code>
     * is of the right type. It returns the result of calling {@link #getEntryInternal(String,
     * String, Class, Object) getEntryInternal} with the specified arguments, as well as {@link
     * #NO_DATA}, converting results of type {@link Primitive} into the associated wrapper type. If
     * the call throws an exception other than an {@link Error} or a {@link ConfigurationException},
     * it throws a <code>ConfigurationException</code> with the original exception as the cause.
     *
     * @throws NoSuchEntryException     {@inheritDoc}
     * @throws IllegalArgumentException {@inheritDoc}
     * @throws NullPointerException     {@inheritDoc}
     */
    public Object getEntry(String component,
                           String name,
                           Class type,
                           Object defaultValue)
            throws ConfigurationException {
        return getEntryInternal(component, name, type, defaultValue, NO_DATA);
    }

    /**
     * Returns an object of the specified type created using the information in the entry matching
     * the specified component and name, and using the specified data (unless it is {@link
     * #NO_DATA}), returning the default value if no matching entry is found and the default value
     * is not {@link #NO_DEFAULT}. If <code>type</code> is a primitive type, then the result is
     * returned as an instance of the associated wrapper class. Repeated calls with the same
     * arguments may or may not return the identical object. <p>
     *
     * The default implementation checks that <code>component</code>, <code>name</code>, and
     * <code>type</code> are not <code>null</code>; that <code>component</code> is a valid qualified
     * identifier; that <code>name</code> is a valid identifier; and that <code>defaultValue</code>
     * is of the right type. It returns the result of calling {@link #getEntryInternal(String,
     * String, Class, Object) getEntryInternal} with the specified arguments, converting results of
     * type {@link Primitive} into the associated wrapper type. If the call throws an exception
     * other than an {@link Error} or a {@link ConfigurationException}, it throws a
     * <code>ConfigurationException</code> with the original exception as the cause.
     *
     * @throws NoSuchEntryException     {@inheritDoc}
     * @throws IllegalArgumentException {@inheritDoc}
     * @throws NullPointerException     {@inheritDoc}
     */
    public Object getEntry(String component,
                           String name,
                           Class type,
                           Object defaultValue,
                           Object data)
            throws ConfigurationException {
        return getEntryInternal(component, name, type, defaultValue, data);
    }

    /**
     * Returns an object created using the information in the entry matching the specified component
     * and name, and the specified data, for the requested type. If the entry value is a primitive,
     * then the object returned should be an instance of {@link Primitive}. Implementations may use
     * <code>type</code> to perform conversions on primitive values, if desired, but are not
     * required to check if the object is of the requested type. Repeated calls with the same
     * arguments may or may not return the identical object. <p>
     *
     * The default implementations of the <code>getEntry</code> methods delegate to this method;
     * implementations can rely on the fact that calls made to this method by those methods will
     * have arguments that are not <code>null</code> and that have the correct syntax.
     *
     * @param component the component being configured
     * @param name      the name of the entry for the component
     * @param type      the type of object requested
     * @param data      an object to use when computing the value of the entry, or {@link #NO_DATA}
     *                  to specify no data
     * @return an object created using the information in the entry matching <code>component</code>
     * and <code>name</code>, and using the value of <code>data</code> (unless it is
     * <code>NO_DATA</code>)
     * @throws NoSuchEntryException   if no matching entry is found
     * @throws ConfigurationException if a matching entry is found but a problem occurs creating the
     *                                object for the entry
     * @throws NullPointerException   if <code>component</code>, <code>name</code>, or
     *                                <code>type</code> is <code>null</code>
     * @see Configuration#getEntry(String, String, Class) Configuration.getEntry
     */
    protected abstract Object getEntryInternal(String component,
                                               String name,
                                               Class type,
                                               Object data)
            throws ConfigurationException;

    /**
     * Helper method, used to implement the public overloadings of getEntry, which checks for null
     * or illegal arguments, and logs and wraps exceptions.
     */
    private Object getEntryInternal(String component,
                                    String name,
                                    Class type,
                                    Object defaultValue,
                                    Object data)
            throws ConfigurationException {
        if (component == null) {
            throw new NullPointerException("component cannot be null");
        } else if (!validQualifiedIdentifier(component)) {
            throw new IllegalArgumentException(
                    "component must be a valid qualified identifier");
        } else if (name == null) {
            throw new NullPointerException("name cannot be null");
        } else if (!validIdentifier(name)) {
            throw new IllegalArgumentException(
                    "name must be a valid identifier");
        } else if (type == null) {
            throw new NullPointerException("type cannot be null");
        } else if (defaultValue != NO_DEFAULT) {
            if (type.isPrimitive()
                    ? (defaultValue == null ||
                    Utilities.getPrimitiveType(defaultValue.getClass()) != type)
                    : (defaultValue != null &&
                    !type.isAssignableFrom(defaultValue.getClass()))) {
                throw new IllegalArgumentException(
                        "defaultValue is of wrong type");
            }
        }
        ConfigurationException configEx;
        try {
            Object result = getEntryInternal(component, name, type, data);
            Class resultType;
            if (result instanceof Primitive) {
                resultType = ((Primitive) result).getType();
                result = ((Primitive) result).getValue();
            } else if (result != null) {
                resultType = result.getClass();
            } else {
                resultType = null;
            }
            if ((resultType == null)
                    ? type.isPrimitive()
                    : !type.isAssignableFrom(resultType)) {
                throw new ConfigurationException(
                        "entry for component " + component + ", name " + name +
                                " is of wrong type: " +
                                Utilities.typeString(resultType));
            }
            if (logger.isLoggable(Level.FINER)) {
                logger.log(Level.FINER,
                        "{0}, component {1}, name {2}" +
                                "{3,choice,0#|1#, data {4}}: returns {5}",
                        new Object[]{
                                this, component, name,
                                new Double(data == NO_DATA ? 0 : 1), data, result
                        });
            }
            return result;
        } catch (NoSuchEntryException e) {
            if (defaultValue == NO_DEFAULT) {
                if (logger.isLoggable(Level.FINE)) {
                    logger.log(Level.FINE,
                            "{0}, component {1}, name {2}: entry not found",
                            new Object[]{this, component, name});
                }
                throw e;
            } else {
                if (logger.isLoggable(Level.FINE)) {
                    logger.log(
                            Level.FINE,
                            "{0}, component {1}, name {2}: returns default {3}",
                            new Object[]{this, component, name, defaultValue});
                }
                return defaultValue;
            }
        } catch (ConfigurationException e) {
            configEx = e;
        } catch (RuntimeException e) {
            configEx = new ConfigurationException(
                    "problem getting entry for component " + component +
                            ", name " + name,
                    e);
        }
        if (logger.isLoggable(Level.FINE)) {
            LogUtils.throwing(logger, this.getClass(), "getEntry", configEx,
                    "{0}, component {1}, name {2}{3,choice,0#|1#, data {4}}: throws",
                    this, component, name, new Double(data == NO_DATA ? 0 : 1), data);
        }
        throw configEx;
    }

    /**
     * Checks if the argument is a valid <i>Identifier</i>, as defined in the <i>Java(TM) Language
     * Specification</i>.
     *
     * @param name the name to check
     * @return <code>true</code> if <code>name</code> is a valid <i>Identifier</i>, else
     * <code>false</code>
     */
    protected static boolean validIdentifier(String name) {
        if (name == null ||
                name.length() == 0 ||
                !Character.isJavaIdentifierStart(name.charAt(0))) {
            return false;
        }
        for (int i = name.length(); --i > 0; ) {
            if (!Character.isJavaIdentifierPart(name.charAt(i))) {
                return false;
            }
        }
        return Arrays.binarySearch(reservedNames, name) < 0;
    }

    /**
     * Checks if the argument is a valid <i>QualifiedIdentifier</i>, as defined in the <i>Java
     * Language Specification</i>.
     *
     * @param name the name to check
     * @return <code>true</code> if <code>name</code> is a valid <i>QualifiedIdentifier</i>, else
     * <code>false</code>
     */
    protected static boolean validQualifiedIdentifier(String name) {
        if (name == null) {
            return false;
        }
        int offset = 0;
        int dot;
        do {
            dot = name.indexOf('.', offset);
            String id = name.substring(
                    offset, dot < 0 ? name.length() : dot);
            if (!validIdentifier(id)) {
                return false;
            }
            offset = dot + 1;
        } while (dot >= 0);
        return true;
    }
}
