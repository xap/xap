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
import net.jini.export.Exporter;
import net.jini.security.ProxyPreparer;
import net.jini.security.Security;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.InvalidObjectException;
import java.io.ObjectInputStream;
import java.io.ObjectStreamException;
import java.io.PrintWriter;
import java.io.Reader;
import java.io.Serializable;
import java.io.StreamTokenizer;
import java.io.StringReader;
import java.io.StringWriter;
import java.lang.reflect.Array;
import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.net.MalformedURLException;
import java.net.URL;
import java.security.PrivilegedAction;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Supplies objects needed to configure applications, such as {@link Exporter} or {@link
 * ProxyPreparer} instances, or application-specific objects, constructed from data in a
 * configuration source and override options, as well as data supplied in the call to
 * <code>getEntry</code>. The configuration source is specified with a file or URL location, or as a
 * character input stream. The contents of the configuration source consist of optional import
 * statements followed by entries, grouped by component, that specify configuration objects using a
 * subset of expression syntax in the Java(TM) programming language. Additional options specify
 * values for individual entries, overriding any matching entries supplied in the configuration
 * source. <p>
 *
 * Applications should normally use {@link ConfigurationProvider} to obtain {@link Configuration}
 * instances, rather than referencing this class directly, so that the interpretation of
 * configuration options can be customized without requiring code modifications. <p>
 *
 * The syntax of a configuration source is as follows, using the same grammar notation that is used
 * in <i>The Java Language Specification (JLS)</i>:
 *
 * <pre>
 * <i>Source</i>:
 *   <i>Imports</i><sub>opt</sub> <i>Components</i><sub>opt</sub>
 *
 * <i>Imports</i>:
 *   <i>Import</i>
 *   <i>Imports</i> <i>Import</i>
 *
 * <i>Import</i>:
 *   import <i>PackageName</i> . * ;
 *   import <i>PackageName</i> . <i>ClassName</i> . * ;
 *   import <i>PackageName</i> . <i>ClassName</i> ;
 *
 * <i>PackageName</i>:
 *   <i>QualifiedIdentifier</i>
 *
 * <i>ClassName</i>:
 *   <i>QualifiedIdentifier</i>
 *
 * <i>Components</i>:
 *   <i>Component</i>
 *   <i>Components</i> <i>Component</i>
 *
 * <i>Component</i>:
 *   <i>QualifiedIdentifier</i> { <i>Entries</i><sub>opt</sub> }
 *
 * <i>Entries</i>:
 *   <i>Entry</i>
 *   <i>Entries</i> <i>Entry</i>
 *
 * <i>Entry</i>:
 *   <i>EntryModifiers</i><sub>opt</sub> <i>Identifier</i> = <i>Expr</i> ;
 *
 * <i>EntryModifiers</i>:
 *   static
 *   private
 *   static private
 *   private static
 *
 * <i>Expr</i>:
 *   <i>Literal</i>
 *   <i>TypeName</i> . class
 *   <i>EntryName</i>
 *   <i>ThisReference</i>
 *   <i>FieldName</i>
 *   <i>Cast</i>
 *   <i>NewExpr</i>
 *   <i>MethodCall</i>
 *   <i>Data</i>
 *   <i>Loader</i>
 *   <i>StringConcatenation</i>
 *
 * <i>Literal</i>:
 *   <i>IntegerLiteral</i>
 *   <i>FloatingPointLiteral</i>
 *   <i>BooleanLiteral</i>
 *   <i>CharacterLiteral</i>
 *   <i>StringLiteral</i>
 *   <i>NullLiteral</i>
 *
 * <i>TypeName</i>:
 *   <i>ClassName</i>
 *   <i>ClassName</i> [ ]
 *   <i>PrimitiveType</i>
 *   <i>PrimitiveType</i> [ ]
 *
 * <i>EntryName</i>:
 *   <i>QualifiedIdentifier</i>
 *
 * <i>ThisReference</i>:
 *   this
 *
 * <i>FieldName</i>:
 *   <i>QualifiedIdentifier</i> . <i>Identifier</i>
 *
 * <i>Cast</i>:
 *   ( <i>TypeName</i> ) <i>Expr</i>
 *
 * <i>NewExpr</i>:
 *   new <i>QualifiedIdentifier</i> ( <i>ExprList</i><sub>opt</sub> )
 *   new <i>QualifiedIdentifier</i> [ ] { <i>ExprList</i><sub>opt</sub> ,<sub>opt</sub> }
 *
 * <i>MethodCall</i>:
 *   <i>StaticMethodName</i> ( <i>ExprList</i><sub>opt</sub> )
 *
 * <i>StaticMethodName</i>:
 *   <i>QualifiedIdentifier</i> . <i>Identifier</i>
 *
 * <i>ExprList</i>:
 *   <i>Expr</i>
 *   <i>ExprList</i> , <i>Expr</i>
 *
 * <i>Data</i>:
 *   $data
 *
 * <i>Loader</i>:
 *   $loader
 *
 * <i>StringConcatenation</i>:
 *   <i>Expr</i> + <i>Expr</i>
 * </pre>
 *
 * The syntax of each override option is as follows:
 *
 * <pre>
 * <i>Override</i>:
 *   <i>EntryModifiers</i><sub>opt</sub> <i>FullyQualifiedEntryName</i> = <i>Expr</i>
 *
 * <i>FullyQualifiedEntryName</i>:
 *   <i>QualifiedIdentifier</i> . <i>Identifier</i>
 * </pre> <p>
 *
 * For example, a simple configuration source file might look like the following:
 *
 * <pre>
 * import java.util.HashSet;
 *
 * com.acme.ContainerUtility {
 *     container = new HashSet(containerSize);
 *     containerSize = 33;
 * }
 * </pre> <p>
 *
 * The productions for <i>BooleanLiteral</i>, <i>CharacterLiteral</i>, <i>FloatingPointLiteral</i>,
 * <i>Identifier</i>, <i>IntegerLiteral</i>, <i>NullLiteral</i>, <i>PrimitiveType</i>,
 * <i>QualifiedIdentifier</i>, and <i>StringLiteral</i> are the same as the ones used in the JLS.
 * <i>StringLiteral</i>s can refer to the values of system properties by using the syntax
 * <code>${<i>propertyName</i>}</code> within the <i>StringLiteral</i>, and can refer to the file
 * name separator character by using the syntax <code>${/}</code>. System property references cannot
 * be nested. Expansion of system properties occurs when the entry is evaluated and, if there is a
 * security manager, will result in its {@link SecurityManager#checkPropertyAccess
 * checkPropertyAccess} method being called with the property name as its argument. Both
 * <i>StringLiteral</i>s and <i>CharacterLiteral</i>s can use character and Unicode escape
 * sequences. Standard comment syntax can also be used throughout. <p>
 *
 * Each <i>Import</i> specifies a class or group of classes which may be referred to using simple
 * names, as specified in the JLS. Classes in the <code>java.lang</code> package are imported by
 * default. <p>
 *
 * Each <i>Component</i> includes <i>Entries</i> which specify expressions to evaluate and return
 * when <code>getEntry</code> is called with the associated component and entry name. More than one
 * <i>Component</i> is allowed to specify the same name; all contribute entries for that component.
 * For a given component, each entry name must be unique. If <i>EntryModifiers</i> contains the
 * <code>static</code> keyword, then the entry is only evaluated once when it is first referenced.
 * Otherwise, entries are evaluated at each reference, including each time an entry is referred to
 * by another entry. Because static entries are only evaluated once (in the access control context
 * of the first caller), care should be taken when passing instances of this class to callers with
 * different access control contexts. If <i>EntryModifiers</i> contains the <code>private</code>
 * keyword, then the entry may be referred to in other entries, but will not be considered by calls
 * to <code>getEntry</code>, which will treat the entry name as not being defined. Entries may have
 * reference, primitive, or <code>null</code> values. Entry values are converted to the requested
 * type by <i>assignment conversion</i>, as defined in the JLS, with the restriction that the value
 * is only considered a constant expression if it is either a <i>StringLiteral</i> with no system
 * property references, another kind of <i>Literal</i>, or an <i>EntryName</i> that refers to
 * another entry whose value is a constant expression. In particular, this restriction means that
 * narrowing primitive conversions are not applied when the value is a reference to a static field,
 * even if the field is a constant. <p>
 *
 * Override options are specified as the second and following elements of the <code>options</code>
 * argument in this class's constructors. Each <i>Override</i> specifies a single entry, using the
 * fully qualified name of the entry (<code><i>component</i>.<i>name</i></code>). The override
 * replaces the matching entry in the configuration source, if any, including both its value and
 * entry modifiers. Each <i>Override</i> option must specify a different
 * <i>FullyQualifiedEntryName</i>. The contents of the <i>Expr</i> are evaluated in the context of
 * any <i>Imports</i> defined in the configuration source. <p>
 *
 * The <i>Expr</i> for each <i>Entry</i> may be specified using a subset of the expression syntax in
 * the Java programming language, supporting literals, references to static fields, casts, class
 * instance creation (using the standard method invocation conversion and selection semantics, but
 * not including creation of anonymous class instances), single dimensional array creation with an
 * array initializer (but not multi-dimensional arrays or arrays declared with an explicit size),
 * and static method invocation using a class name (also using standard method invocation conversion
 * and selection semantics, but not permitting methods with a <code>void</code> return type).
 * Expressions are interpreted in the unnamed package, although only public members may be accessed.
 * The <code>this</code> expression may be used to refer to the containing
 * <code>ConfigurationFile</code> instance itself. <p>
 *
 * The use of the <code>+</code> operator in a configuration source is also allowed, but it may be
 * used only for string concatenation, as defined by the JLS.  Using the <code>+</code> operator in
 * an arithmetic expression results in a <code>ConfigurationException</code> being thrown.<p>
 *
 * The <code>ConfigurationFile</code> class provides built-in support for two <i>special entry
 * expressions</i>, which are <i>Identifier</i>s that start with <code>'$'</code>. The
 * <code>$data</code> expression, of type {@link Object}, may be used to refer to the
 * <code>data</code> argument specified in a call to <code>getEntry</code>. Only non-static entries
 * may refer to <code>$data</code> or other entries that refer to <code>$data</code>. Calling
 * <code>getEntry</code> without specifying <code>$data</code> results in a
 * <code>ConfigurationException</code> being thrown if the associated entry refers to
 * <code>$data</code>. The <code>$loader</code> expression, of type {@link ClassLoader}, may be used
 * to refer to the <code>ClassLoader</code> specified when creating the
 * <code>ConfigurationFile</code>. If the <code>ConfigurationFile</code> was created using the
 * context class loader either by not specifying a class loader or by specifying <code>null</code>
 * for the class loader, then the caller must be granted {@link RuntimePermission}<code>("getClassLoader")</code>
 * in order to evaluate an entry that refers to <code>$loader</code>. Subclasses can provide support
 * for additional special entry expressions by supplying implementations of {@link
 * #getSpecialEntryType getSpecialEntryType} and {@link #getSpecialEntry getSpecialEntry}. <p>
 *
 * Entry expressions may also refer to other entries by name, using the simple entry name for
 * entries within the same component, and the fully qualified entry name for entries in any
 * component. A fully qualified name for which there is both an entry and a valid static field is
 * interpreted as referring to the entry. An unqualified entry name for which there is an entry
 * within the same component and is specified as a special entry expression will be interpreted as
 * referring to the entry within that component. <p>
 *
 * Calls to the following methods are prohibited in order to avoid incorrect behavior because of
 * their reliance on determining the <code>ClassLoader</code> or <code>AccessControlContext</code>
 * of the caller:
 *
 * <ul> <li> <code>java.lang.Class.forName</code> <li> <code>java.lang.ClassLoader.getSystemClassLoader</code>
 * <li> <code>java.lang.Package.getPackage</code> <li> <code>java.lang.Package.getPackages</code>
 * <li> <code>java.lang.System.load</code> <li> <code>java.lang.System.loadLibrary</code> <li>
 * <code>java.security.AccessController.doPrivileged</code> <li> <code>java.sql.DriverManager.deregisterDriver</code>
 * <li> <code>java.sql.DriverManager.getConnection</code> <li> <code>java.sql.DriverManager.getDriver</code>
 * <li> <code>java.sql.DriverManager.getDrivers</code> <li> <code>net.jini.security.Security.doPrivileged</code>
 * </ul>
 *
 * Attempting to evaluate an entry that calls any of these methods results in
 * <code>ConfigurationException</code> being thrown. Additional prohibited methods may be specified
 * by providing a resource named "net/jini/config/resources/ConfigurationFile.moreProhibitedMethods".
 * Each line in the resource file should specify an additional prohibited method, represented by the
 * fully qualified class name, a <code>'.'</code>, and the method name for an additional prohibited
 * method, with no spaces. The resource file must be encoded in UTF-8. <p>
 *
 * Any syntax error or problem reading from the configuration source or an override option results
 * in a <code>ConfigurationException</code> being thrown. <p>
 *
 * If there is a security manager, the configuration source refers to the members of a class, and
 * the class is in a named package, then this class calls the security manager's {@link
 * SecurityManager#checkPackageAccess checkPackageAccess} method with the class's package. Making
 * this call in <code>ConfigurationFile</code> insures that the check is made despite any decisions
 * within reflection to skip the check based on the class of the caller, which in this case will be
 * <code>ConfigurationFile</code> rather than its caller. Note that implementations are permitted to
 * make calls to reflection to access class members at arbitrary stack depths relative to that of
 * the caller of <code>ConfigurationFile</code>; applications using security managers with custom
 * implementations of the {@link SecurityManager#checkMemberAccess checkMemberAccess} method should
 * take this behavior into account.
 *
 * @author Sun Microsystems, Inc.
 * @since 2.0
 *
 *
 * This implementation uses the {@link Logger} named <code>net.jini.config</code> to log information
 * at the following logging levels: <p>
 *
 * <table border="1" cellpadding="5" summary="Describes logging performed by the ConfigurationFile
 * class at different logging levels">
 *
 * <caption halign="center" valign="top"><b><code> net.jini.config</code></b></caption>
 *
 * <tr> <th scope="col"> Level <th scope="col"> Description
 *
 * <tr> <td> {@link Level#INFO INFO} <td> problems adding new prohibited methods
 *
 * <tr> <td> {@link Level#FINE FINE} <td> problems getting entries, including getting entries
 * that are not found
 *
 * <tr> <td> {@link Level#FINE FINE} <td> returning default values
 *
 * <tr> <td> {@link Level#FINER FINER} <td> creating an instance of this class, getting existing
 * entries, or adding new prohibited methods
 *
 * </table>
 */
@com.gigaspaces.api.InternalApi
public class ConfigurationFile extends AbstractConfiguration {

    /* -- Fields -- */

    /**
     * The primitive types, in increasing order with respect to widening conversions.
     */
    private static final Class[] primitives = {
            Boolean.TYPE, Byte.TYPE, Character.TYPE, Short.TYPE, Integer.TYPE,
            Long.TYPE, Float.TYPE, Double.TYPE
    };

    /**
     * Index of Integer.TYPE in primitives.
     */
    private static final int INT_INDEX = 4;

    /**
     * Index of Byte.TYPE in primitives.
     */
    private static final int BYTE_INDEX = 1;

    /**
     * The binary names of public, static methods that may not be called from within a configuration
     * source, for all overloads.  These methods are prohibited because they depend on knowing the
     * class loader or access control context of their caller, which will be those of
     * ConfigurationFile rather than of the caller of the ConfigurationFile constructor or
     * getEntry.
     *
     * XXX: Update list if new methods like these are added.  Find these methods by searching for
     * ClassLoader.getCallerClassLoader, System.getCallerClass or Reflection.getCallerClass, which
     * are used to make permission checks based on the caller's class loader.  Note that there would
     * be more kinds of security holes if ConfigurationFile permitted calls to instance methods.
     * -tjb[5.Aug.2002]
     */
    private static final Set prohibitedMethods =
            new HashSet(Arrays.asList(new String[]{
                    "java.lang.Class.forName",
                    "java.lang.ClassLoader.getSystemClassLoader",
                    "java.lang.Package.getPackage",
                    "java.lang.Package.getPackages",
                    "java.lang.System.load",
                    "java.lang.System.loadLibrary",
                    "java.security.AccessController.doPrivileged",
                    "java.sql.DriverManager.deregisterDriver",
                    "java.sql.DriverManager.getConnection",
                    "java.sql.DriverManager.getDriver",
                    "java.sql.DriverManager.getDrivers",
                    "net.jini.security.Security.doPrivileged"}));

    /**
     * The name of the resource that specifies more prohibited methods.
     */
    private static final String moreProhibitedMethods =
            "net/jini/config/resources/ConfigurationFile.moreProhibitedMethods";

    /* Add names of more prohibited methods. */
    static {
        InputStream in = null;
        try {
            ClassLoader cl = ConfigurationFile.class.getClassLoader();
            if (cl == null) {
                cl = Utilities.bootstrapResourceLoader;
            }
            in = cl.getResourceAsStream(moreProhibitedMethods);
            if (in != null) {
                BufferedReader reader =
                        new BufferedReader(new InputStreamReader(in, "utf-8"));
                while (true) {
                    String line = reader.readLine();
                    if (line == null) {
                        break;
                    }
                    logger.log(Level.FINER, "Adding prohibited method: {0}",
                            line);
                    prohibitedMethods.add(line);
                }
            }
        } catch (IOException e) {
            logger.log(
                    Level.INFO, "Problem reading prohibited methods resource", e);
        } finally {
            if (in != null) {
                try {
                    in.close();
                } catch (IOException e) {
                }
            }
        }
    }

    /**
     * Returns the current context class loader.
     */
    private static final PrivilegedAction contextClassLoader =
            new PrivilegedAction() {
                public Object run() {
                    return Thread.currentThread().getContextClassLoader();
                }
            };

    /**
     * Permission needed to get the context class loader.
     */
    static final RuntimePermission getClassLoaderPermission =
            new RuntimePermission("getClassLoader");

    /**
     * Map from entry names to Entry instances.
     */
    final Map entries = new HashMap(11);

    /**
     * Map of simple class names to the full class names that were explicitly imported.
     */
    final Map<String, String> classImports = new HashMap<String, String>(1);

    /**
     * List of packages or classes whose member classes should be imported on demand.
     */
    final List<String> onDemandImports = new ArrayList<String>(1);

    /**
     * The location of the configuration source, or null if not specified.
     */
    private String location;

    /**
     * The override being parsed, or zero if not parsing an override.
     */
    private int override = 0;

    /**
     * The class loader to use to resolve classes.
     */
    private final ClassLoader cl;

    /**
     * Whether a non-null class loader was supplied.
     */
    private final boolean nonNullLoaderSupplied;

    /**
     * Lock for synchronizing calls to resolve entries.
     */
    private final Object resolveLock = new Object();

    /* -- Classes -- */

    /**
     * Defines a StreamTokenizer that resets sval, nval, and lineno when the pushBack method is
     * called.
     */
    private static class PushbackStreamTokenizer extends StreamTokenizer {
        private boolean gotToken;
        private boolean pushedBack;
        private double savedNval;
        private String savedSval;
        private int savedTtype;
        private int savedLineno;

        PushbackStreamTokenizer(Reader reader) {
            super(reader);
        }

        @Override
        public int nextToken() throws IOException {
            if (pushedBack) {
        /* Use values saved by pushBack */
                nval = savedNval;
                sval = savedSval;
                ttype = savedTtype;
                pushedBack = false;
            } else {
        /* Save values for pushBack */
                savedNval = nval;
                savedSval = sval;
                savedTtype = ttype;
                savedLineno = lineno();
                super.nextToken();
                gotToken = true;
            }
            return ttype;
        }

        @Override
        public void pushBack() {
            if (gotToken && !pushedBack) {
		/*
		 * Save values for next nextToken call and reestablish previous
		 * values.
		 */
                double tempNval = savedNval;
                savedNval = nval;
                nval = tempNval;
                String tempSval = savedSval;
                savedSval = sval;
                sval = tempSval;
                int tempTtype = savedTtype;
                savedTtype = ttype;
                ttype = tempTtype;
                pushedBack = true;
            }
        }

        @Override
        public int lineno() {
	    /* Use the previous value if pushed back */
            return pushedBack ? savedLineno : super.lineno();
        }
    }

    /**
     * Base class to represent parse results.
     */
    private abstract class ParseNode {

        /* The line number of the text parsed -- for error reporting */
        final int lineno;

        /*
         * The override of the text parsed, or zero if not an override -- for
         * error reporting.
         */
        final int override;

        ParseNode(int lineno) {
            this.lineno = lineno;
            this.override = ConfigurationFile.this.override;
        }

        /**
         * Calculates and returns the declared type of the parse node, as referred to by the
         * specified entry.
         */
        abstract Class resolve(Entry inEntry) throws ConfigurationException;

        /**
         * Returns true if the value is a constant.
         */
        abstract boolean isConstant() throws ConfigurationException;

        /**
         * Returns the result of evaluating the parse node with the specified data provided by the
         * call to getEntry.
         */
        abstract Object eval(Object data) throws ConfigurationException;

        /**
         * Throws a ConfigurationException for the an error described by the what argument, using
         * the line number and override for this parse node.
         */
        void oops(String what) throws ConfigurationException {
            ConfigurationFile.this.oops(what, lineno, override, null);
        }

        /**
         * Throws a ConfigurationException for the an error described by the what argument, at the
         * specified line number, with the override for this parse node.
         */
        void oops(String what, int lineno) throws ConfigurationException {
            ConfigurationFile.this.oops(what, lineno, override, null);
        }

        /**
         * Throws a ConfigurationException for the an error described by the what argument, using
         * the line number and override for this parse node, and caused by the specified exception,
         * which may be null.
         */
        void oops(String what, Throwable t) throws ConfigurationException {
            ConfigurationFile.this.oops(what, lineno, override, t);
        }
    }

    /**
     * Represents an entry.
     */
    private class Entry extends ParseNode {
        final String component;
        final String fullName;
        private final ParseNode node;
        final boolean isPrivate;
        final boolean isStatic;
        final boolean isOverride;
        private Class type;
        private boolean resolved;	/* resolve done */
        private boolean resolving;	/* resolve in progress */
        private boolean isConstant;
        private boolean refersToData;	/* computed in parse and resolve */
        private boolean evaluated;	/* static eval done */
        private boolean evaluating;	/* static eval in progress */
        private Object value;

        Entry(String component,
              String fullName,
              boolean isPrivate,
              boolean isStatic,
              boolean isOverride,
              int lineno,
              Parser parser)
                throws ConfigurationException, IOException {
            super(lineno);
            this.component = component;
            this.fullName = fullName;
            this.isPrivate = isPrivate;
            this.isStatic = isStatic;
            this.isOverride = isOverride;
            node = parser.parseExpr(this);
        }

        boolean getRefersToData() {
            return refersToData;
        }

        void setRefersToData() {
            refersToData = true;
        }

        @Override
        Class resolve(Entry inEntry) throws ConfigurationException {
	    /*
	     * Grab a single lock when resolving any entry.  Until we know that
	     * the entry contains no textually circular references, which is
	     * determined during this resolve step, there could be a deadlock
	     * if two threads were to lock different entries at the same time
	     * that refer to each other.  -tjb[5.Sep.2002]
	     */
            synchronized (resolveLock) {
                if (!resolved) {
                    if (resolving) {
                        oops("entry with circular reference: " + fullName);
                    }
                    resolving = true;
                    try {
                        type = node.resolve(this);
                        isConstant = node.isConstant();
                    } finally {
                        resolving = false;
                    }
                    resolved = true;
                }
            }
            return type;
        }

        @Override
        boolean isConstant() throws ConfigurationException {
            resolve(this);
            return isConstant;
        }

        @Override
        Object eval(Object data) throws ConfigurationException {
            resolve(this);
            if (!isStatic) {
                return node.eval(data);
            }
	    /*
	     * Grab a separate lock when evaluating each static entry.  Using
	     * separate locks permits entries to be evaluated simultaneously if
	     * they don't refer to each other, which is important if the entry
	     * expressions perform network or other long running operations.
	     * Note that there is still the possibility of a deadlock if there
	     * is a circular dependency between static entries via recursive
	     * calls to getEntry.  -tjb[5.Sep.2002]
	     */
            synchronized (this) {
                if (!evaluated) {
                    if (evaluating) {
                        oops("entry with circular reference: " + fullName);
                    }
                    evaluating = true;
                    try {
                        value = node.eval(NO_DATA);
                    } finally {
                        evaluating = false;
                    }
                    evaluated = true;
                }
            }
            return value;
        }
    }

    /**
     * Represents a reference to an entry, data specified by getEntry, or a field.
     */
    private class NameRef extends ParseNode {
        private final String name;
        private final String fullName; /* The fully qualified entry name */
        private Entry entry;
        private Field field;

        NameRef(String component, String name, int lineno) {
            super(lineno);
            this.name = name;
            fullName = name.indexOf('.') < 0 ? component + '.' + name : name;
        }

        @Override
        Class resolve(Entry inEntry) throws ConfigurationException {
            entry = (Entry) entries.get(fullName);
            if (entry != null) {
                Class result = entry.resolve(inEntry);
                if (entry.getRefersToData()) {
                    if (inEntry.isStatic) {
                        oops("static entry '" + inEntry.fullName +
                                "' cannot refer to entry '" + fullName +
                                "', which uses '$data'");
                    }
                    inEntry.setRefersToData();
                }
                return result;
            } else if (name.equals("$data")) {
                return Object.class;
            } else if (name.equals("$loader")) {
                return ClassLoader.class;
            } else if (name.startsWith("$") && name.indexOf('.') == -1) {
                try {
                    return getSpecialEntryType(name);
                } catch (NoSuchEntryException e) {
                    oops("entry not found: " + name, lineno);
                } catch (ConfigurationException e) {
                    oops("problem referring to entry '" + name + "'", e);
                } catch (RuntimeException e) {
                    oops("problem referring to entry '" + name + "'", e);
                }
                return null;	/* Not reached */
            } else {
                field = findField(name, lineno, override);
                return field.getType();
            }
        }

        @Override
        boolean isConstant() throws ConfigurationException {
	    /*
	     * There's no way to tell that a field is a compile-time constant,
	     * so only return true for entries.
	     */
            return entry != null && entry.isConstant();
        }

        @Override
        Object eval(Object data) throws ConfigurationException {
            if (entry != null) {
                return entry.eval(data);
            } else if (field != null) {
                try {
                    return field.get(null);
                } catch (IllegalAccessException e) {
                    oops("problem accessing field '" + name + "'", e);
                    return null; /* Not reached */
                }
            } else if (name.equals("$data")) {
                if (data == NO_DATA) {
                    oops("no data specified for '$data'");
                }
                return data;
            } else if (name.equals("$loader")) {
                if (!nonNullLoaderSupplied) {
                    SecurityManager sm = System.getSecurityManager();
                    if (sm != null) {
                        sm.checkPermission(getClassLoaderPermission);
                    }
                }
                return cl;
            } else {
                try {
                    return getSpecialEntry(name);
                } catch (ConfigurationException e) {
                    oops("problem referring to entry '" + name + "'", e);
                } catch (RuntimeException e) {
                    oops("problem referring to entry '" + name + "'", e);
                }
                return null;	/* Not reached */
            }
        }
    }

    /**
     * Represents a simple literal value.
     */
    private class Literal extends ParseNode {
        private final Class type;
        private final Object value;

        Literal(Class type, Object value, int lineno) {
            super(lineno);
            this.type = type;
            this.value = value;
        }

        Class resolve(Entry inEntry) {
            return type;
        }

        @Override
        boolean isConstant() {
            return true;
        }

        @Override
        Object eval(Object data) {
            return value;
        }
    }

    /**
     * Represents a reference to this ConfigurationFile.
     */
    private class ThisRef extends Literal {
        ThisRef(int lineno) {
            super(ConfigurationFile.class, ConfigurationFile.this, lineno);
        }
    }

    /**
     * Represents a String literal.
     */
    private class StringLiteral extends ParseNode {
        private final String value;

        StringLiteral(String value, int lineno) {
            super(lineno);
            this.value = value;
        }

        @Override
        Class resolve(Entry inEntry) {
            return String.class;
        }

        @Override
        boolean isConstant() {
            return value.indexOf("${") < 0;
        }

        @Override
        Object eval(Object data) throws ConfigurationException {
            return expandStringProperties(value, lineno);
        }
    }

    /**
     * Represents a Class literal.
     */
    private class ClassLiteral extends ParseNode {
        private final String className;
        private final boolean isArray;
        private Class value;

        ClassLiteral(String className, boolean isArray, int lineno) {
            super(lineno);
            this.className = className;
            this.isArray = isArray;
        }

        @Override
        Class resolve(Entry inEntry) throws ConfigurationException {
            Class c = findClass(className, lineno, override);
            if (isArray) {
                c = Array.newInstance(c, 0).getClass();
            }
            value = c;
            return Class.class;
        }

        @Override
        boolean isConstant() {
            return true;
        }

        @Override
        Object eval(Object data) {
            return value;
        }
    }

    /**
     * Represents a cast.
     */
    private class Cast extends ParseNode {
        private final String typeName;
        private final boolean isArray;
        private final ParseNode arg;
        private Class type;

        Cast(String typeName, boolean isArray, ParseNode arg, int lineno) {
            super(lineno);
            this.typeName = typeName;
            this.isArray = isArray;
            this.arg = arg;
        }

        Class resolve(Entry inEntry) throws ConfigurationException {
            type = findClass(typeName, lineno, override);
            if (isArray) {
                type = Array.newInstance(type, 0).getClass();
            }
            Class argType = arg.resolve(inEntry);
            if (type == argType) {
                return type;
            } else if (type.isPrimitive()) {
                if (type != Boolean.TYPE &&
                        argType != Boolean.TYPE &&
                        argType != null &&
                        argType.isPrimitive()) {
		    /* Cast primitive to primitive, not boolean */
                    return type;
                }
            } else if (argType == null ||
                    type.isAssignableFrom(argType) ||
                    argType.isAssignableFrom(type)) {
		/* Null source, or source and target are related */
                return type;
            } else if (type.isInterface() && argType.isInterface()) {
                if (compatibleMethods(type, argType)) {
		    /* Both interfaces, compatible methods */
                    return type;
                }
            } else if ((type.isInterface() &&
                    !argType.isArray() &&
                    !Modifier.isFinal(argType.getModifiers())) ||
                    (argType.isInterface() &&
                            !type.isArray() &&
                            !Modifier.isFinal(type.getModifiers()))) {
		/* Non-final class and interface */
                return type;
            }
            oops("cannot convert '" + Utilities.typeString(argType) +
                    "' to '" + Utilities.typeString(type) + "'");
            return null;	/* Not reached */
        }

        boolean isConstant() throws ConfigurationException {
            return arg.isConstant();
        }

        @Override
        Object eval(Object data) throws ConfigurationException {
            Object val = arg.eval(data);
            if (type.isPrimitive()) {
                if (Utilities.getPrimitiveType(val.getClass()) != type) {
                    val = convertPrimitive(val, type);
                }
            } else if (val != null && !type.isInstance(val)) {
                oops("cannot convert '" +
                        Utilities.typeString(val.getClass()) + "' to '" +
                        Utilities.typeString(type) + "'");
            }
            return val;
        }
    }

    /**
     * Describes a call to a constructor, method, array constructor, or string concatenation
     * operation.
     */
    private abstract class Call extends ParseNode {
        final ParseNode[] args;

        Call(ParseNode[] args, int lineno) {
            super(lineno);
            this.args = args;
        }

        boolean isConstant() {
            return false;
        }

        /**
         * Returns the declared types of the argument expressions.
         */
        Class[] resolveArgs(Entry inEntry) throws ConfigurationException {
            Class[] types = new Class[args.length];
            for (int i = 0; i < args.length; i++) {
                types[i] = args[i].resolve(inEntry);
            }
            return types;
        }

        /**
         * Evaluates the arguments.
         */
        Object[] evalArgs(Object data) throws ConfigurationException {
            Object[] result = new Object[args.length];
            for (int i = 0; i < args.length; i++) {
                result[i] = args[i].eval(data);
            }
            return result;
        }
    }

    private class StringConcatenation extends Call {

        StringConcatenation(ParseNode[] args, int lineno) {
            super(args, lineno);
        }

        Class resolve(Entry inEntry) throws ConfigurationException {
            Class[] types = resolveArgs(inEntry);
            if (!(types[0] == String.class || types[1] == String.class)) {
                oops("The static type of the first or second operand in a"
                        + " string concatenation expression must be"
                        + " java.lang.String.  The static types of the first"
                        + " and second operands in the string concatenation"
                        + " expression are " + types[0] + " and " + types[1]
                        + ", respectively");
            }
            return String.class;
        }

        boolean isConstant() {
            return false;
        }

        Object eval(Object data) throws ConfigurationException {
            StringBuffer value = new StringBuffer();
            Object[] evaledArgs = evalArgs(data);
            for (int i = 0; i < evaledArgs.length; i++) {
                Object o1 = evaledArgs[i];
                value.append(o1);
            }
            return value.toString();
        }
    }

    /**
     * Describes a Constructor call
     */
    private class ConstructorCall extends Call {

        /**
         * The name of the class to construct
         */
        private final String typeName;

        private Constructor constructor;

        ConstructorCall(String typeName, ParseNode[] args, int lineno) {
            super(args, lineno);
            this.typeName = typeName;
        }

        Class resolve(Entry inEntry) throws ConfigurationException {
            constructor = findConstructor(
                    typeName, resolveArgs(inEntry), lineno, override);
            return constructor.getDeclaringClass();
        }

        Object eval(Object data) throws ConfigurationException {
            Object[] evaluatedArgs = evalArgs(data);
            Throwable except;
            try {
                return constructor.newInstance(evaluatedArgs);
            } catch (InvocationTargetException e) {
                except = e.getTargetException();
                if (except instanceof Error) {
                    throw (Error) except;
                }
            } catch (Exception e) {
                except = e;
            }
            oops("problem invoking constructor for " + typeName, except);
            return null;	/* Not reached */
        }
    }

    /**
     * Describes a method call.
     */
    private class MethodCall extends Call {

        /**
         * The full name of the method
         */
        final String fullName;

        private Method method;

        MethodCall(String fullName, ParseNode[] args, int lineno) {
            super(args, lineno);
            this.fullName = fullName;
        }

        Class resolve(Entry inEntry) throws ConfigurationException {
            method = findMethod(
                    fullName, resolveArgs(inEntry), lineno, override);
            Class c = method.getReturnType();
            if (c == Void.TYPE) {
                oops("method has void return type: " + fullName);
            }
            return c;
        }

        Object eval(Object data) throws ConfigurationException {
            Object[] evaluatedArgs = evalArgs(data);
            Throwable except;
            try {
                return method.invoke(null, evaluatedArgs);
            } catch (InvocationTargetException e) {
                except = e.getTargetException();
                if (except instanceof Error) {
                    throw (Error) except;
                }
            } catch (Exception e) {
                except = e;
            }
            oops("problem invoking method " + fullName, except);
            return null;	/* Not reached */
        }
    }

    /**
     * Represents constructing and initializing a single dimensional array.
     */
    private class ArrayConstructor extends Call {

        /* The name of the element type. */
        private final String typeName;

        /* The element type, when resolved. */
        private Class type;

        ArrayConstructor(String typeName, ParseNode[] args, int lineno) {
            super(args, lineno);
            this.typeName = typeName;
        }

        Class resolve(Entry inEntry) throws ConfigurationException {
            type = findClass(typeName, lineno, override);
            Class[] types = resolveArgs(inEntry);
            for (int i = 0; i < types.length; i++) {
                if (!assignable(types[i], type)) {
                    if (args[i] instanceof Literal) {
			/* Handle primitive narrowing conversion */
                        Object narrow =
                                narrowingAssignable(args[i].eval(NO_DATA), type);
                        if (narrow != null) {
                            args[i] = new Literal(
                                    Utilities.getPrimitiveType(narrow.getClass()),
                                    narrow, args[i].lineno);
                            continue;
                        }
                    }
                    oops("array element " + i + " has incorrect type",
                            args[i].lineno);
                }
            }
            return Array.newInstance(type, 0).getClass();
        }

        Object eval(Object data) throws ConfigurationException {
            Object[] contents = evalArgs(data);
            Object result = Array.newInstance(type, args.length);
            for (int i = 0; i < contents.length; i++) {
                Object value = contents[i];
                Class valueType = value == null ? null : value.getClass();
                if (!type.isPrimitive() && !assignable(valueType, type)) {
		    /* Runtime array assignment check for Object arrays. */
                    oops("array element " + i + " has incorrect type",
                            args[i].lineno);
                }
                Array.set(result, i, value);
            }
            return result;
        }
    }

    /**
     * Parses the input from the specified Reader and options, storing information about imports and
     * entries by side effect.
     */
    private class Parser {

        /**
         * The tokenizer to use for parsing
         */
        private StreamTokenizer st;

        /**
         * Adds the imports and entries parsed from the specified input stream, as well as overrides
         * parsed from the specified options, starting with the second element.
         */
        Parser(Reader reader, String[] options) throws ConfigurationException {
            try {
                onDemandImports.add("java.lang");
                if (reader != null) {
                    if (!(reader instanceof BufferedReader)) {
                        reader = new BufferedReader(reader);
                    }
                    createTokenizer(reader);
                    parseSource();
                }
                for (int i = 1; i < options.length; i++) {
                    override = i;
                    if (options[i].startsWith("-"))
                        continue;
                    createTokenizer(new StringReader(options[i]));
                    parseOverride();
                }
            } catch (IOException e) {
                ConfigurationFile.this.oops(
                        "problem reading configuration file", 0, 0, e);
            } finally {
                override = 0;
            }
        }

        /**
         * Creates and sets the tokenizer using the specified reader.
         */
        private void createTokenizer(Reader reader) {
            reader = new UnicodeEscapesDecodingReader(reader);
            st = new PushbackStreamTokenizer(reader);
            st.ordinaryChar('.');
            st.wordChars('.', '.');
            st.ordinaryChars('0', '9');
            st.wordChars('0', '9');
            st.ordinaryChar('-');
            st.wordChars('-', '-');
            st.wordChars('_', '_');
            st.wordChars('$', '$');
            st.ordinaryChar('/');
            st.slashSlashComments(true);
            st.slashStarComments(true);
        }

        /**
         * Parses imports and components from the source, and stores the results.
         */
        private void parseSource() throws ConfigurationException, IOException {
            while (true) {
                int t = st.nextToken();
                if (t == StreamTokenizer.TT_EOF) {
                    break;
                }
                st.pushBack();
                String val = token(
                        "keyword 'import', 'static' or 'private', or component");
                if ("import".equals(val)) {
                    parseImport();
                } else {
                    parseComponent(val);
                }
            }
        }

        /**
         * Parses an Import, updating classImports and onDemandImports.
         */
        private void parseImport() throws ConfigurationException, IOException {
            if (!entries.isEmpty()) {
                oops("import follows entry");
            }
	    /* Include asterisks */
            st.wordChars('*', '*');
            String value = token("import package or type");
            st.ordinaryChar('*');
            boolean onDemand = value.endsWith(".*");
            if (onDemand) {
                value = value.substring(0, value.length() - 2);
            }
            if (!validQualifiedIdentifier(value)) {
                syntax("import package or type");
            }
            token(';');
            if (onDemand) {
                if (!onDemandImports.contains(value)) {
                    Class c = findClassNoImports(value, st.lineno(), override);
                    if (c != null && c.getName().indexOf('.') < 0) {
                        oops("import from unnamed package");
                    }
                    onDemandImports.add(value);
                }
            } else {
                int dot = value.lastIndexOf('.');
                if (dot < 0) {
                    oops("import from unnamed package");
                }
                Class c = findClassNoImports(value, st.lineno(), override);
                if (c == null) {
                    oops("class not found: " + value);
                } else if (c.getName().indexOf('.') < 0) {
                    oops("import from unnamed package");
                }
                String simple = value.substring(dot + 1);
                if (classImports.containsKey(simple) &&
                        !classImports.get(simple).equals(value)) {
                    oops("conflicting imports: " + classImports.get(simple) +
                            " and " + value);
                }
                classImports.put(simple, value);
            }
        }

        /**
         * Parses a Component.
         */
        private void parseComponent(String component)
                throws ConfigurationException, IOException {
            if (!validQualifiedIdentifier(component)) {
                oops("invalid component: '" + component + "'");
            }
            token('{');
            while (true) {
                int t = st.nextToken();
                if (t == StreamTokenizer.TT_WORD) {
                    parseEntry(component, st.sval);
                } else if (t == '}') {
                    break;
                } else {
                    syntax("'}', keyword 'static' or 'private', " +
                            "or entry name");
                }
            }
        }

        /**
         * Parses an Entry.
         */
        private void parseEntry(String component, String name)
                throws ConfigurationException, IOException {
            boolean isPrivate = false;
            boolean isStatic = false;
            while (true) {
                if ("private".equals(name)) {
                    if (isPrivate) {
                        oops("duplicate 'private'");
                    }
                    isPrivate = true;
                } else if ("static".equals(name)) {
                    if (isStatic) {
                        oops("duplicate 'static'");
                    }
                    isStatic = true;
                } else {
                    break;
                }
                name = token("keyword 'static' or 'private', or entry name");
            }
            if (!validIdentifier(name)) {
                oops("illegal entry name: " + name);
            }
            String fullName = component + '.' + name;
            if (entries.containsKey(fullName)) {
                oops("duplicate entry name: " + name);
            }
            token('=');
            Entry entry = new Entry(component, fullName, isPrivate, isStatic,
                    false /* isOverride */, st.lineno(), this);
            token(';');
            entries.put(fullName, entry);
        }

        /**
         * Parses an Override .
         */
        private void parseOverride()
                throws ConfigurationException, IOException {
            boolean isPrivate = false;
            boolean isStatic = false;
            String name;
            while (true) {
                name = token("keyword 'static' or 'private', " +
                        "or fully qualified entry name");
                if ("private".equals(name)) {
                    if (isPrivate) {
                        oops("duplicate 'private'");
                    }
                    isPrivate = true;
                } else if ("static".equals(name)) {
                    if (isStatic) {
                        oops("duplicate 'static'");
                    }
                    isStatic = true;
                } else {
                    break;
                }
            }
            int dot = name.lastIndexOf('.');
            if (!validQualifiedIdentifier(name) || dot < 0) {
                syntax("fully qualified entry name");
            }
            Entry entry = (Entry) entries.get(name);
            if (entry != null && entry.isOverride) {
                oops("duplicate override: " + name);
            }
            token('=');
            String component = name.substring(0, dot);
            entry = new Entry(component, name, isPrivate, isStatic,
                    true /* isOverride */, st.lineno(), this);
            int t = st.nextToken();
            if (t != StreamTokenizer.TT_EOF) {
                syntax("no more characters");
            }
            entries.put(name, entry);
        }

        /**
         * Parses an ExprList and the trailing token specified by close.
         */
        private ParseNode[] parseArgs(Entry inEntry, char close)
                throws ConfigurationException, IOException {
            List args = new ArrayList(1);
            int t = st.nextToken();
            boolean arrayArgs = close == '}';
            if (t != close) {
                if (t == ',' && arrayArgs) {
                    t = st.nextToken();
                    if (t != close) {
                        syntax("'" + close + "'");
                    }
                } else {
                    st.pushBack();
                    while (true) {
                        args.add(parseExpr(inEntry));
                        t = st.nextToken();
                        if (t == close) {
                            break;
                        } else if (t != ',') {
                            syntax("',' or '" + close + "'");
                        } else if (arrayArgs) {
                            t = st.nextToken();
                            if (t == close) {
                                break;
                            } else {
                                st.pushBack();
                            }
                        }
                    }
                }
            }
            return (ParseNode[]) args.toArray(new ParseNode[args.size()]);
        }

        /**
         * Parses a string concatenation
         */
        ParseNode parseExpr(Entry inEntry)
                throws ConfigurationException, IOException {
            List nodes = new ArrayList();
            for (; ; ) {
                nodes.add(parseSubExpr(inEntry));
                if (st.nextToken() != '+') {
                    st.pushBack();
                    break;
                }
            }
            if (nodes.size() == 1) {
                return (ParseNode) nodes.get(0);
            } else {
                return new StringConcatenation(
                        (ParseNode[]) nodes.toArray(new ParseNode[nodes.size()]),
                        st.lineno());
            }
        }

        /**
         * Parses an Expr.
         */
        ParseNode parseSubExpr(Entry inEntry)
                throws ConfigurationException, IOException {
            int t = st.nextToken();
            String val = st.sval;
            if (t == '\'') {
                if (val.length() != 1) {
                    oops("invalid character: '" + val + "'");
                }
                return new Literal(Character.TYPE,
                        new Character(val.charAt(0)), st.lineno());
            } else if (t == '"') {
                return new StringLiteral(val, st.lineno());
            } else if (t == '(') {
                int lineno = st.lineno();
                String type = token("type");
                t = st.nextToken();
                boolean isArray = false;
                if (t == '[') {
                    isArray = true;
                    token(']');
                    t = st.nextToken();
                }
                if (t != ')') {
                    syntax("')'");
                }
                return new Cast(type, isArray, parseExpr(inEntry), lineno);
            }
            st.pushBack();
            val = token("expression");
            if (Character.isDigit(val.charAt(0)) || val.charAt(0) == '-') {
                return getNumber(val);
            } else if ("true".equals(val)) {
                return new Literal(Boolean.TYPE, Boolean.TRUE, st.lineno());
            } else if ("false".equals(val)) {
                return new Literal(Boolean.TYPE, Boolean.FALSE, st.lineno());
            } else if ("null".equals(val)) {
                return new Literal(null, null, st.lineno());
            } else if ("new".equals(val)) {
                return parseNewInstance(inEntry);
            } else if ("this".equals(val)) {
                return new ThisRef(st.lineno());
            } else if (val.endsWith(".class")) {
                val = val.substring(0, val.length() - 6);
                if (findPrimitiveClass(val) == null &&
                        !validQualifiedIdentifier(val)) {
                    oops("illegal class name: " + val);
                }
                return new ClassLiteral(val, false, st.lineno());
            }
            t = st.nextToken();
            if (t == '[') {
                int lineno = st.lineno();
                token(']');
                if (!".class".equals(token("'.class'"))) {
                    syntax("'.class'");
                } else if (findPrimitiveClass(val) == null &&
                        !validQualifiedIdentifier(val)) {
                    oops("illegal class name: " + val);
                }
                return new ClassLiteral(val, true, lineno);
            }
            st.pushBack();
            if (!validQualifiedIdentifier(val)) {
                if (t == '(') {
                    oops("illegal method name: " + val);
                } else {
                    oops("illegal field or entry name: " + val);
                }
            }
            if (t == '(') {
                return parseMethodCall(inEntry, val);
            } else {
                if ("$data".equals(val)) {
                    if (inEntry.isStatic) {
                        oops("cannot use '$data' in a static entry");
                    }
                    inEntry.setRefersToData();
                }
                return new NameRef(inEntry.component, val, st.lineno());
            }
        }

        /**
         * Parses a numeric literal and returns the value as a Literal.
         */
        private Literal getNumber(String val)
                throws ConfigurationException, IOException {
            char c = val.charAt(val.length() - 1);
            try {
                if (c == 'l' || c == 'L') {
                    return new Literal(
                            Long.TYPE,
                            Long.decode(val.substring(0, val.length() - 1)),
                            st.lineno());
                } else if (c == 'f' || c == 'F') {
                    return new Literal(
                            Float.TYPE,
                            Float.valueOf(val.substring(0, val.length() - 1)),
                            st.lineno());
                } else if (c == 'd' || c == 'D') {
                    return new Literal(
                            Double.TYPE,
                            Double.valueOf(val.substring(0, val.length() - 1)),
                            st.lineno());
                } else if (val.indexOf('e') > 0 ||
                        val.indexOf('E') > 0 ||
                        val.indexOf('.') > 0) {
                    return new Literal(
                            Double.TYPE, Double.valueOf(val), st.lineno());
                } else {
                    return new Literal(
                            Integer.TYPE, Integer.decode(val), st.lineno());
                }
            } catch (NumberFormatException e) {
                oops("bad numeric literal: " + val);
                return null;    // Not reached
            }
        }

        /**
         * Parses a NewExpr except for the leading "new", and returns the constructed object.
         */
        private ParseNode parseNewInstance(Entry inEntry)
                throws ConfigurationException, IOException {
            int lineno = st.lineno();
            String typeName = token("type name");
            int t = st.nextToken();
            if (t == '[') {
                token(']');
                token('{');
                return new ArrayConstructor(
                        typeName, parseArgs(inEntry, '}'), lineno);
            } else if (t != '(') {
                syntax("'(' or '['");
            }
            return new ConstructorCall(
                    typeName, parseArgs(inEntry, ')'), lineno);
        }

        /**
         * Resolves a static method call and returns a MethodCall instance that describes the method
         * and arguments for the call.
         */
        private MethodCall parseMethodCall(Entry inEntry, String name)
                throws ConfigurationException, IOException {
            if (!validQualifiedIdentifier(name) || name.indexOf('.') < 0) {
                syntax("method name");
            }
            int lineno = st.lineno();
            token('(');
            return new MethodCall(name, parseArgs(inEntry, ')'), lineno);
        }

        /**
         * Parses the next token from the stream, and generates a syntax error if the token does not
         * equal the specified character.  Expands references to system properties if the token is a
         * String.
         */
        private void token(char c) throws ConfigurationException, IOException {
            int t = st.nextToken();
            if (t != c) {
                if (c == '"') {
                    syntax("a String");
                } else {
                    syntax(new String(new char[]{'\'', c, +'\''}));
                }
            }
        }

        /**
         * Parses the next token from the stream, and generates a syntax error if the token is not a
         * TT_WORD.  The what argument describes what kind of token is expected.  Combines multiple
         * TT_WORD tokens if they are separated by '.' at the boundaries and are not reserved
         * words.
         */
        private String token(String what)
                throws ConfigurationException, IOException {
            int t = st.nextToken();
            if (t != StreamTokenizer.TT_WORD) {
                syntax(what);
            }
            if (st.sval.indexOf('.') >= 0 ||
                    validIdentifier(st.sval) ||
                    findPrimitiveClass(st.sval) != null) {
                String result = st.sval;
                while (true) {
                    int p = st.nextToken();
                    if (p != StreamTokenizer.TT_WORD ||
                            (!result.endsWith(".") && !st.sval.startsWith("."))) {
                        st.pushBack();
                        break;
                    }
                    result += st.sval;
                }
                st.sval = result;
            }
            return st.sval;
        }

        /**
         * Throws a ConfigurationException for a syntax error described by the what argument.
         */
        private void syntax(String what) throws ConfigurationException {
            oops("expected " + what + ", found " + describeCurrentToken());
        }

        /**
         * Returns a String that describes the current token.
         */
        private String describeCurrentToken() {
            switch (st.ttype) {
                case StreamTokenizer.TT_EOF:
                    return "end of file";
                case StreamTokenizer.TT_EOL:
                    return "end of line";
                case StreamTokenizer.TT_WORD:
                    return "'" + st.sval + "'";
                case StreamTokenizer.TT_NUMBER:
                    return "'" + st.nval + "'";
                case '"':
                    return "\"" + st.sval + "\"";
                case '\'':
                    return "'" + st.sval.charAt(0) + "'";
                default:
                    return "'" + ((char) st.ttype) + "'";
            }
        }

        /**
         * Throws a ConfigurationException for an error described by the what argument.
         */
        private void oops(String what) throws ConfigurationException {
            ConfigurationFile.this.oops(what, st.lineno(), override, null);
        }
    }

    /* -- Constructors -- */

    /**
     * Creates an instance containing the entries specified by the options, using the calling
     * thread's context class loader for interpreting class names. The first option is a file or URL
     * specifying the location of the configuration source; no source is specified if
     * <code>options</code> is <code>null</code>, empty, or has <code>"-"</code> as the first
     * element. The remaining options specify values for individual entries, overriding any matching
     * entries supplied in the configuration source.
     *
     * @param options an array whose first element is the location of the configuration source and
     *                remaining elements specify override values for entries
     * @throws ConfigurationNotFoundException if the specified source location cannot be found
     * @throws ConfigurationException         if <code>options</code> is not <code>null</code> and
     *                                        any of its elements is <code>null</code>; or if there
     *                                        is a syntax error in the contents of the source or the
     *                                        overrides; or if there is an I/O exception reading
     *                                        from the source; or if the calling thread does not
     *                                        have permission to read the specified source file,
     *                                        connect to the specified URL, or read the system
     *                                        properties referred to in the source or overrides. Any
     *                                        <code>Error</code> thrown while creating the instance
     *                                        is propagated to the caller; it is not wrapped in a
     *                                        <code>ConfigurationException</code>.
     */
    public ConfigurationFile(String[] options) throws ConfigurationException {
        this(options, null);
    }

    /**
     * Creates an instance containing the entries specified by the options, using the specified
     * class loader for interpreting class names. The first option is a file or URL specifying the
     * location of the configuration source; no source is specified if <code>options</code> is
     * <code>null</code>, empty, or has <code>"-"</code> as the first element. The remaining options
     * specify values for individual entries, overriding any matching entries supplied in the
     * configuration source. Specifying <code>null</code> for the class loader uses the current
     * thread's context class loader.
     *
     * @param options an array whose first element is the location of the configuration source and
     *                remaining elements specify override values for entries
     * @param cl      the class loader to use for interpreting class names. If <code>null</code>,
     *                uses the context class loader.
     * @throws ConfigurationNotFoundException if the specified source location cannot be found
     * @throws ConfigurationException         if <code>options</code> is not <code>null</code> and
     *                                        any of its elements is <code>null</code>; or if there
     *                                        is a syntax error in the contents of the source or the
     *                                        overrides; or if there is an I/O exception reading
     *                                        from the source; or if the calling thread does not
     *                                        have permission to read the specified source file,
     *                                        connect to the specified URL, or read the system
     *                                        properties referred to in the source or overrides. Any
     *                                        <code>Error</code> thrown while creating the instance
     *                                        is propagated to the caller; it is not wrapped in a
     *                                        <code>ConfigurationException</code>.
     */
    public ConfigurationFile(String[] options, ClassLoader cl)
            throws ConfigurationException {
        options = checkOptions(options);
        nonNullLoaderSupplied = cl != null;
        this.cl = nonNullLoaderSupplied ? cl
                : (ClassLoader) Security.doPrivileged(contextClassLoader);
        if (location == null) {
            new Parser(null, options);
        } else {
            InputStream in = null;
            try {
                try {
                    URL url = new URL(location);
                    in = url.openStream();
                } catch (MalformedURLException e) {
                    in = new FileInputStream(location);
                }
                new Parser(new InputStreamReader(in), options);
            } catch (FileNotFoundException e) {
                ErrorDescriptor ed = new ErrorDescriptor(0, 0,
                        "configuration file not found", location, e);
                throwConfigurationException(
                        new ConfigurationNotFoundException(ed.toString(), e),
                        Collections.singletonList(ed));
                throw new AssertionError("throwConfigurationException must"
                        + " throw an exception");
            } catch (IOException e) {
                oops("problem reading configuration file", 0, 0, e);
            } catch (RuntimeException e) {
                oops("problem reading configuration file", 0, 0, e);
            } finally {
                if (in != null) {
                    try {
                        in.close();
                    } catch (IOException e) {
                    }
                }
            }
        }
        logger.log(Level.FINER, "created {0}", this);
    }

    /**
     * Checks that options contains no nulls, returning a non-null value.  If the argument is not
     * null, not empty, and its first element is not "-", sets location to the first element.
     */
    private String[] checkOptions(String[] options)
            throws ConfigurationException {
        if (options == null) {
            return new String[0];
        } else if (options.length > 0) {
            for (int i = 0; i < options.length; i++) {
                if (options[i] == null) {
                    oops("option is null", 0, i);
                }
            }
            if (!"-".equals(options[0])) {
                location = options[0];
            }
        }
        return options;
    }

    /**
     * Creates an instance containing the entries parsed from the specified character stream and
     * options, using the calling thread's context class loader for interpreting class names. The
     * constructor completes reading from the character input stream before returning, but does not
     * close the stream.  It is the responsibility of the caller to ensure that the character input
     * stream is closed.  The first option is used for the location of the configuration source when
     * reporting errors; no location is specified if <code>options</code> is <code>null</code>,
     * empty, or has <code>"-"</code> as the first element. The remaining options specify values for
     * individual entries, overriding any matching entries supplied in the configuration source.
     *
     * @param reader  the character input stream
     * @param options an array whose first element is the location of the configuration source and
     *                remaining elements specify override values for entries
     * @throws ConfigurationException if <code>options</code> is not <code>null</code> and any of
     *                                its elements is <code>null</code>, or if there is a syntax
     *                                error in the contents of the character input stream or the
     *                                overrides, or if there is an I/O exception reading from the
     *                                input stream, or if the calling thread does not have
     *                                permission to read the system properties referred to in the
     *                                input stream or overrides. Any <code>Error</code> thrown while
     *                                creating the instance is propagated to the caller; it is not
     *                                wrapped in a <code>ConfigurationException</code>.
     * @throws NullPointerException   if <code>reader</code> is <code>null</code>
     */
    public ConfigurationFile(Reader reader, String[] options)
            throws ConfigurationException {
        this(reader, options, null);
    }

    /**
     * Creates an instance containing the entries parsed from the specified character stream and
     * options, using the specified class loader for interpreting class names. The constructor
     * completes reading from the character input stream before returning, but does not close the
     * stream.  It is the responsibility of the caller to ensure that the character input stream is
     * closed. The first option is used for the location of the configuration source when reporting
     * errors; no location is specified if <code>options</code> is <code>null</code>, empty, or has
     * <code>"-"</code> as the first element. The remaining options specify values for individual
     * entries, overriding any matching entries supplied in the configuration source. Specifying
     * <code>null</code> for the class loader uses the current thread's context class loader.
     *
     * @param reader  the character input stream
     * @param options an array whose first element is the location of the configuration source and
     *                remaining elements specify override values for entries
     * @param cl      the class loader to use for interpreting class names. If <code>null</code>,
     *                uses the context class loader.
     * @throws ConfigurationException if <code>options</code> is not <code>null</code> and any of
     *                                its elements is <code>null</code>, or if there is a syntax
     *                                error in the contents of the character input stream or the
     *                                overrides, or if there is an I/O exception reading from the
     *                                input stream, or if the calling thread does not have
     *                                permission to read the system properties referred to in the
     *                                input stream or overrides. Any <code>Error</code> thrown while
     *                                creating the instance is propagated to the caller; it is not
     *                                wrapped in a <code>ConfigurationException</code>.
     * @throws NullPointerException   if <code>reader</code> is <code>null</code>
     */
    public ConfigurationFile(Reader reader, String[] options, ClassLoader cl)
            throws ConfigurationException {
        if (reader == null) {
            throw new NullPointerException("reader is null");
        }
        options = checkOptions(options);
        nonNullLoaderSupplied = cl != null;
        this.cl = nonNullLoaderSupplied ? cl
                : (ClassLoader) Security.doPrivileged(contextClassLoader);
        new Parser(reader, options);
        logger.log(Level.FINER, "created {0}", this);
    }

    /**
     * Allows a subclass of <code>ConfigurationFile</code> to control the
     * <code>ConfigurationException</code> that is thrown.  It must be called any time the
     * <code>ConfigurationFile</code> implementation encounters a situation that would trigger a
     * <code>ConfigurationException</code>.  Such situations occur when attempting to parse a
     * configuration source or when attempting to return an entry or the type of an entry and are
     * fully documented in the specification for each <code>ConfigurationFile</code> method that
     * throws a <code>ConfigurationException</code>. <p> The default <code>ConfigurationFile</code>
     * implementation throws <code>defaultException</code> if <code>defaultException</code> is not
     * <code>null</code>.  If <code>defaultException</code> is <code>null</code>, the default
     * implementation throws a <code>ConfigurationException</code> constructed from the error
     * descriptors provided in <code>errors</code>.  The default implementation throws
     * <code>java.lang.NullPointerException</code> if <code>errors</code> is null,
     * <code>java.lang.IllegalArgumentException</code> if <code>errors</code> is empty, and
     * <code>java.lang.ClassCastException</code> if the contents of <code>errors</code> is not
     * assignable to {@link ConfigurationFile.ErrorDescriptor ErrorDescriptor}.
     *
     * @param defaultException the exception the <code>ConfigurationFile</code> implementation would
     *                         normally throw.
     * @param errors           list of errors encountered in parsing the configuration source or
     *                         when attempting to return an entry or the type of an entry. This
     *                         parameter may not be null, it must contain at least one element, and
     *                         the elements of the list must be assignable to {@link
     *                         ConfigurationFile.ErrorDescriptor ErrorDescriptor}.  The order in
     *                         which the errors appear in the list reflects the order in which they
     *                         were encountered by the <code>ConfigurationFile</code>
     *                         implementation.
     * @throws ConfigurationException the subclass-specific <code>ConfigurationException</code> or
     *                                the default exception passed in by the <code>ConfigurationFile</code>
     *                                implementation.
     * @since 2.1
     */
    protected void throwConfigurationException(
            ConfigurationException defaultException, List errors)
            throws ConfigurationException {
        if (errors == null)
            throw new NullPointerException("errors parameter cannot be null");
        if (errors.isEmpty())
            throw new IllegalArgumentException(
                    "error list must contain at least one error");
        ConfigurationException exception = null;
        if (errors.size() > 1) {
            StringWriter sw = new StringWriter();
            PrintWriter pw = new PrintWriter(sw);
            for (Iterator it = errors.iterator(); it.hasNext(); ) {
                ErrorDescriptor error = (ErrorDescriptor) it.next();
                pw.print("\n" + error.toString());
                Throwable cause = error.getCause();
                if (cause != null) {
                    pw.print("\ncaused by: ");
                    cause.printStackTrace(pw);
                }
            }
            pw.flush();
            exception = new ConfigurationException(sw.toString());
        } else {
            ErrorDescriptor error = (ErrorDescriptor) errors.iterator().next();
            exception = new ConfigurationException(error.toString(),
                    error.getCause());
        }
        if (defaultException != null)
            throw defaultException;
        throw exception;
    }    
    
    /* -- Accessor methods -- */

    /**
     * Returns an object created using the information in the entry matching the specified component
     * and name, and the specified data, for the requested type. If the entry value is a primitive,
     * then the object returned is an instance of {@link AbstractConfiguration.Primitive}. This
     * implementation uses <code>type</code> to perform conversions on primitive values. Repeated
     * calls with the same arguments may or may not return the identical object.<p>
     *
     * @throws NoSuchEntryException {@inheritDoc}
     * @throws NullPointerException {@inheritDoc}
     */
    @Override
    protected Object getEntryInternal(String component,
                                      String name,
                                      Class type,
                                      Object data)
            throws ConfigurationException {
        if (component == null || name == null || type == null) {
            throw new NullPointerException(
                    "component, name and type cannot be null");
        }
        Entry entry = (Entry) entries.get(component + '.' + name);
        if (entry == null || entry.isPrivate) {
            oopsNoSuchEntry("entry not found for component " + component +
                    ", name " + name);
        }
        Class entryType = entry.resolve(null);
        if (!assignable(entryType, type)) {
            if (entryType != null &&
                    entryType.isPrimitive() &&
                    entry.isConstant()) {
		/* Handle primitive narrowing conversion */
                Object narrow = narrowingAssignable(entry.eval(NO_DATA), type);
                if (narrow != null) {
                    return new Primitive(narrow);
                }
            }
            oops("entry of wrong type for component " + component +
                            ", name " + name +
                            ": expected " + Utilities.typeString(type) +
                            ", found " + Utilities.typeString(entryType), entry.lineno,
                    entry.override);
        }
        Object result = entry.eval(data);
        if (entryType != null && entryType.isPrimitive()) {
            if (entryType != type) {
                result = convertPrimitive(result, type);
            }
            return new Primitive(result);
        } else {
            return result;
        }
    }

    /**
     * Returns a set containing the fully qualified names of all non-private entries defined for
     * this instance.
     *
     * @return a set containing the fully qualified names of all non-private entries defined for
     * this instance
     */
    public Set getEntryNames() {
        Set result = new HashSet(entries.size());
        for (Iterator iter = entries.values().iterator(); iter.hasNext(); ) {
            Entry entry = (Entry) iter.next();
            if (!entry.isPrivate) {
                result.add(entry.fullName);
            }
        }
        return result;
    }

    /**
     * Returns the static type of the expression specified for the entry with the specified
     * component and name.
     *
     * @param component the component
     * @param name      the name of the entry for the component
     * @return the static type of the matching entry, or <code>null</code> if the value of the entry
     * is the <code>null</code> literal
     * @throws NoSuchEntryException     if no matching entry is found
     * @throws ConfigurationException   if a matching entry is found but a problem occurs
     *                                  determining the type of the entry. Any <code>Error</code>
     *                                  thrown while processing the entry is propagated to the
     *                                  caller; it is not wrapped in a <code>ConfigurationException</code>.
     * @throws IllegalArgumentException if <code>component</code> is not <code>null</code> and is
     *                                  not a valid <i>QualifiedIdentifier</i>, or if
     *                                  <code>name</code> is not <code>null</code> and is not a
     *                                  valid <i>Identifier</i>
     * @throws NullPointerException     if either argument is <code>null</code>
     */
    public Class getEntryType(String component, String name)
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
        }
        Entry entry = (Entry) entries.get(component + '.' + name);
        if (entry == null || entry.isPrivate) {
            if (logger.isLoggable(Level.FINE)) {
                logger.log(Level.FINE,
                        "entry for component {0}, name {1} not found in {2}",
                        new Object[]{component, name, this});
            }
            oopsNoSuchEntry("entry not found for component " + component +
                    ", name " + name);
        }
        ConfigurationException configEx;
        try {
            Class result = entry.resolve(null);
            if (logger.isLoggable(Level.FINER)) {
                logger.log(Level.FINER,
                        "{0}, component {1}, name {2}: returns {3}",
                        new Object[]{this, component, name, result});
            }
            return result;
        } catch (ConfigurationException e) {
            configEx = e;
        } catch (RuntimeException e) {
            String description = "problem getting type of entry for component "
                    + component + ", name " + name;
            configEx = null;
            try {
                oops(description, entry.lineno, entry.override, e);
            } catch (ConfigurationException ce) {
                configEx = ce;
            }
        }
        if (logger.isLoggable(Level.FINE)) {
            String message = LogUtils.format(this.getClass(), "getEntryType",
                    "{0}, component {1}, name {2}: throws", this, component, name);
            logger.log(Level.FINE, message, configEx);

        }
        throw configEx;
    }

    /**
     * Returns the type of the special entry with the specified name. Special entry names may be
     * referred to from within other entries, but will not be considered by calls to
     * <code>getEntry</code>.  Implementations can assume that the argument to this method is a
     * valid special entry name, which is an <i>Identifier</i> that starts with <code>'$'</code>.
     * This method will be called when attempting to determine the type of a name expression that is
     * a valid special entry name, does not refer to an existing entry, and is not a special entry
     * expression supported by <code>ConfigurationFile</code>. The object returned by a call to
     * {@link #getSpecialEntry getSpecialEntry} with the same argument must be an instance of the
     * class returned by this method. <p>
     *
     * The default implementation always throws <code>NoSuchEntryException</code>.
     *
     * @param name the name of the special entry
     * @return the type of the special entry
     * @throws NoSuchEntryException   if the special entry is not found
     * @throws ConfigurationException if there is a problem determining the type of the special
     *                                entry
     */
    protected Class getSpecialEntryType(String name)
            throws ConfigurationException {
        oopsNoSuchEntry("entry not found: " + name);
        return null;
    }

    /**
     * Returns the value of the special entry with the specified name. Special entry names may be
     * referred to from within other entries, but will not be considered by calls to
     * <code>getEntry</code>. Implementations can assume that the argument to this method is a valid
     * special entry name, which is an <i>Identifier</i> that starts with <code>'$'</code>. This
     * method will be called when attempting to determine the value of a name expression which is a
     * valid special entry name, does not refer to an existing entry, and is not a special entry
     * expression supported by <code>ConfigurationFile</code>. The object returned by this method
     * must be an instance of the class returned by a call to {@link #getSpecialEntryType
     * getSpecialEntryType} with the same argument. <p>
     *
     * The default implementation always throws <code>NoSuchEntryException</code>.
     *
     * @param name the name of the special entry
     * @return the value of the special entry
     * @throws NoSuchEntryException   if the special entry is not found
     * @throws ConfigurationException if there is a problem evaluating the special entry
     */
    protected Object getSpecialEntry(String name)
            throws ConfigurationException {
        oopsNoSuchEntry("entry not found: " + name);
        return null;
    }

    /* -- Utilities -- */

    /**
     * Resolves a type name to a Class.  Primitive type names are supported.
     */
    Class findClass(String name, int lineno, int override)
            throws ConfigurationException {
        return findClass(name, lineno, override, false /* returnIfNotFound */);
    }

    /**
     * Resolves a type name to a Class.  Primitive type names are supported. If returnIfNotFound is
     * true, then returns null rather than throwing an exception if the class is not found.
     */
    Class findClass(String name,
                    int lineno,
                    int override,
                    boolean returnIfNotFound)
            throws ConfigurationException {
        Class primitiveClass = findPrimitiveClass(name);
        if (primitiveClass != null) {
            return primitiveClass;
        }
	/*
	 * Try class imports.  If the name refers to an inner class, then find
	 * the topmost class in the name specified.
	 */
        int dot = name.indexOf('.');
        String n = dot < 0 ? name : name.substring(0, dot);
        String classImport = classImports.get(n);
        if (classImport != null) {
            if (dot >= 0) {
                classImport += name.substring(dot).replace('.', '$');
            }
            Class c = findClassNoImports(classImport, lineno, override);
            if (c != null) {
                return c;
            }
        }
	/* Try class name as is */
        Class c = findClassNoImports(name, lineno, override);
        if (c != null) {
            return c;
        }
	/*
	 * Try on demand imports.  If the name refers to an inner class, then
	 * replace all dots with dollars.
	 */
        String inner = name.replace('.', '$');
        Class found = null;
        for (int i = onDemandImports.size(); --i >= 0; ) {
            String pkgOrClass = onDemandImports.get(i);
            for (int j = 0; j < 2; j++) {
                char sep = j == 0 ? '.' : '$';
                Class next = findClassExact(
                        pkgOrClass + sep + inner, lineno, override);
                if (next != null) {
                    if (found != null) {
                        oops("ambiguous class: " + name, lineno, override);
                    }
                    found = next;
                }
            }
        }
        if (found == null && !returnIfNotFound) {
            oops("class not found: " + name, lineno, override);
        }
        return found;
    }

    /**
     * Returns the class if it names a primitive, otherwise null.
     */
    static Class findPrimitiveClass(String name) {
        if (name.indexOf('.') < 0) {
            for (int i = primitives.length; --i >= 0; ) {
                if (name.equals(primitives[i].getName())) {
                    return primitives[i];
                }
            }
        }
        return null;
    }

    /**
     * Returns the class with the specified name, or null if not found, ignoring imports, but
     * looking for nested classes.  Throws a ConfigurationException if the class is found but is not
     * public, or has a non-public declaring class.
     */
    Class findClassNoImports(String name, int lineno, int override)
            throws ConfigurationException {
        Class c = findClassExact(name, lineno, override);
        if (c != null) {
            return c;
        }
        int dot;
        while ((dot = name.lastIndexOf('.')) >= 0) {
            name = name.substring(0, dot) + '$' + name.substring(dot + 1);
            c = findClassExact(name, lineno, override);
            if (c != null) {
                return c;
            }
        }
        return null;
    }

    /**
     * Returns the class with this precise name, or null if not found, ignoring imports and not
     * substituting '$' for nested classes.  Throws a ConfigurationException if the class is found
     * but is not public, or has a non-public declaring class.
     */
    Class findClassExact(String name, int lineno, int override)
            throws ConfigurationException {
        try {
            Class result = Class.forName(name, false, cl);
            Class c = result;
            do {
                if (!Modifier.isPublic(c.getModifiers())) {
                    name = name.replace('$', '.');
                    if (c == result) {
                        oops("class not public: " + name, lineno, override);
                    } else {
                        oops("class " + name + " is not accessible: " +
                                        "declaring class " +
                                        c.getName().replace('$', '.') + " is not public",
                                lineno, override);
                    }
                }
                c = c.getDeclaringClass();
            } while (c != null);
            return result;
        } catch (ClassNotFoundException e) {
            return null;
        }
    }

    /**
     * Resolves a field name to a field.
     */
    Field findField(String name, int lineno, int override)
            throws ConfigurationException {
        int dot = name.lastIndexOf('.');
        if (dot < 0) {
            oops("entry not found: " + name, lineno, override);
        }
        String className = name.substring(0, dot);
        Class c = findClass(
                className, lineno, override, true /* returnIfNotFound */);
        if (c == null) {
            oops("entry or field not found: " + name, lineno, override);
        }
        String fieldName = name.substring(dot + 1);
        checkPackageAccess(c);
        try {
            Field field = c.getField(fieldName);
            if (!Modifier.isStatic(field.getModifiers())) {
                oops(fieldName + " is not a static field of class " +
                        className, lineno, override);
            }
            return field;
        } catch (NoSuchFieldException e) {
            oops(fieldName + " is not a public field of class " + className,
                    lineno, override);
            return null;	/* Not reached */
        }
    }

    /**
     * Check for permission to access the package of the specified class, if any.  Call this method
     * prior to using reflection to access class members to insure that the access check is not
     * skipped because of caller-based checks using ConfigurationFile instead of the caller of
     * ConfigurationFile.
     */
    private static void checkPackageAccess(Class type) {
        SecurityManager sm = System.getSecurityManager();
        if (sm != null) {
            String name = type.getName();
            int i = name.lastIndexOf('.');
            if (i != -1) {
                sm.checkPackageAccess(name.substring(0, i));
            }
        }
    }

    /**
     * Resolves a method name to a method
     */
    Method findMethod(String fullName,
                      Class[] argumentTypes,
                      int lineno,
                      int override)
            throws ConfigurationException {
        int dot = fullName.lastIndexOf('.');
        String typeName = fullName.substring(0, dot);
        String methodName = fullName.substring(dot + 1);
        Class type = findClass(typeName, lineno, override, true);
        if (type == null) {
            oops("declaring class: " + typeName + ", for method: "
                    + fullName + " was not found", lineno, override);
        }
        Method method = null;
        checkPackageAccess(type);
        Method[] meths = type.getMethods();
        boolean check = false;
        for (int i = meths.length; --i >= 0; ) {
            if (!methodName.equals(meths[i].getName()) ||
                    !Modifier.isStatic(meths[i].getModifiers()) ||
                    !compatible(meths[i].getParameterTypes(), argumentTypes)) {
                continue;
            } else if (method == null) {
                method = meths[i];
            } else if (assignable(meths[i].getParameterTypes(),
                    method.getParameterTypes()) &&
                    assignable(meths[i].getDeclaringClass(),
                            method.getDeclaringClass())) {
                method = meths[i];
            } else {
                check = true;
            }
        }
        if (method == null) {
            oops("no applicable public static method found: " +
                            type.getName() + '.' + methodName +
                            typesString(argumentTypes),
                    lineno, override);
        } else if (prohibitedMethods.contains(
                method.getDeclaringClass().getName() + '.' +
                        methodName)) {
            oops("call to method prohibited: " + fullName, lineno, override);
        } else if (check) {
            for (int i = meths.length; --i >= 0; ) {
                if (meths[i] == method ||
                        !methodName.equals(meths[i].getName()) ||
                        !Modifier.isStatic(meths[i].getModifiers()) ||
                        !compatible(meths[i].getParameterTypes(), argumentTypes)) {
                    continue;
                } else if (!assignable(method.getParameterTypes(),
                        meths[i].getParameterTypes()) ||
                        !assignable(method.getDeclaringClass(),
                                meths[i].getDeclaringClass())) {
                    oops("ambiguous method invocation: " + type.getName() +
                                    '.' + methodName + typesString(argumentTypes),
                            lineno, override);
                }
            }
        }
        return method;
    }

    /**
     * Resolve a type name to a constructor
     */
    Constructor findConstructor(String typeName,
                                Class[] argumentTypes,
                                int lineno,
                                int override)
            throws ConfigurationException {
        Class type = findClass(typeName, lineno, override);
        if (Modifier.isInterface(type.getModifiers())) {
            oops("calling constructor for interface class: " + type.getName(),
                    lineno, override);
        } else if (type.isPrimitive()) {
            oops("calling constructor for primitive class: " + type.getName(),
                    lineno, override);
        } else if (Modifier.isAbstract(type.getModifiers())) {
            oops("calling constructor for abstract class: " + type.getName(),
                    lineno, override);
        }
        checkPackageAccess(type);
        Constructor[] cons = type.getConstructors();
        Constructor constructor = null;
        boolean check = false;
        for (int i = cons.length; --i >= 0; ) {
            if (!compatible(cons[i].getParameterTypes(), argumentTypes)) {
                continue;
            } else if (constructor == null ||
                    assignable(cons[i].getParameterTypes(),
                            constructor.getParameterTypes())) {
                constructor = cons[i];
            } else {
                check = true;
            }
        }
        if (constructor == null) {
            oops("no public constructor found: " + type.getName() +
                            typesString(argumentTypes),
                    lineno, override);
        } else if (check) {
            for (int i = cons.length; --i >= 0; ) {
                if (cons[i] != constructor &&
                        compatible(cons[i].getParameterTypes(), argumentTypes) &&
                        !assignable(constructor.getParameterTypes(),
                                cons[i].getParameterTypes())) {
                    oops("ambiguous constructor invocation: " +
                                    type.getName() + typesString(argumentTypes),
                            lineno, override);
                }
            }
        }
        return constructor;
    }

    /**
     * Returns true if methods or constructors with the specified parameter types can accept
     * arguments of the specified types.
     */
    private static boolean compatible(Class[] parameterTypes,
                                      Class[] argumentTypes) {
        return parameterTypes.length == argumentTypes.length &&
                assignable(argumentTypes, parameterTypes);
    }

    /**
     * Returns true if values of the types specified in "from" can be passed to parameters of the
     * types specified in "to". The two arrays are assumed to be of the same length. A null type
     * represents a null value.
     */
    private static boolean assignable(Class[] from, Class[] to) {
        for (int i = from.length; --i >= 0; ) {
            if (!assignable(from[i], to[i])) {
                return false;
            }
        }
        return true;
    }

    /**
     * Returns true if values of type "from" can be passed to a parameter of type "to". A null
     * "from" type represents a null value. For primitive types, widening conversions are, with one
     * exception, from types other than boolean and byte (the first legal type is char) and are only
     * to the types int, long, float, or double. The one exception is the byte to short conversion,
     * which is checked for separately.
     */
    static boolean assignable(Class from, Class to) {
        if (from == null) {
            return !to.isPrimitive();
        } else if (to.isAssignableFrom(from)) {
            return true;
        } else if (to.isPrimitive() && from.isPrimitive()) {
            if (from == Byte.TYPE && to == Short.TYPE) {
                return true;
            }
            for (int j = primitives.length; --j >= INT_INDEX; ) {
                if (to == primitives[j]) {
                    while (--j >= BYTE_INDEX) {
                        if (from == primitives[j]) {
                            return true;
                        }
                    }
                    break;
                }
            }
        }
        return false;
    }

    /**
     * Returns the value to assign if the primitive value represented by the wrapper object in
     * "from" can be assigned to parameters of type "to" using a narrowing primitive conversion,
     * else null.
     */
    Object narrowingAssignable(Object from, Class to) {
        if (from instanceof Byte ||
                from instanceof Character ||
                from instanceof Short ||
                from instanceof Integer) {
            int val = (from instanceof Character) ?
                    ((Character) from).charValue() : ((Number) from).intValue();
            if (to == Byte.TYPE) {
                if (val >= Byte.MIN_VALUE && val <= Byte.MAX_VALUE) {
                    return new Byte((byte) val);
                }
            } else if (to == Character.TYPE) {
                if (val >= Character.MIN_VALUE &&
                        val <= Character.MAX_VALUE) {
                    return new Character((char) val);
                }
            } else if (to == Short.TYPE) {
                if (val >= Short.MIN_VALUE && val <= Short.MAX_VALUE) {
                    return new Short((short) val);
                }
            }
        }
        return null;
    }

    /**
     * Converts a wrapper instance to an instance of the wrapper class for the specified primitive
     * type.
     */
    static Object convertPrimitive(Object obj, Class primitiveType) {
        Number n = obj instanceof Character
                ? new Integer(((Character) obj).charValue())
                : (Number) obj;
        if (primitiveType == Byte.TYPE) {
            return new Byte(n.byteValue());
        } else if (primitiveType == Character.TYPE) {
            return new Character((char) n.intValue());
        } else if (primitiveType == Short.TYPE) {
            return new Short(n.shortValue());
        } else if (primitiveType == Integer.TYPE) {
            return new Integer(n.intValue());
        } else if (primitiveType == Long.TYPE) {
            return new Long(n.longValue());
        } else if (primitiveType == Float.TYPE) {
            return new Float(n.floatValue());
        } else if (primitiveType == Double.TYPE) {
            return new Double(n.doubleValue());
        } else {
            return null;
        }
    }

    /**
     * Returns true if the two classes have no public methods with the same name and parameter types
     * but different return types.
     */
    static boolean compatibleMethods(Class c1, Class c2) {
        Method[] meths = c1.getMethods();
        for (int i = meths.length; --i >= 0; ) {
            Method m1 = meths[i];
            try {
                Method m2 = c2.getMethod(m1.getName(), m1.getParameterTypes());
                if (m2 != null && m1.getReturnType() != m2.getReturnType()) {
                    return false;
                }
            } catch (NoSuchMethodException e) {
            }
        }
        return true;
    }

    /**
     * Returns a String describing the types in an argument list.
     */
    private static String typesString(Class[] types) {
        StringBuffer sb = new StringBuffer();
        sb.append('(');
        for (int i = 0; i < types.length; i++) {
            if (i != 0) {
                sb.append(", ");
            }
            sb.append(Utilities.typeString(types[i]));
        }
        sb.append(')');
        return sb.toString();
    }

    /**
     * Expands properties embedded in a string with ${some.property.name}. Also treats ${/} as
     * ${file.separator}.
     */
    String expandStringProperties(String value, int lineno)
            throws ConfigurationException {
        int p = value.indexOf("${", 0);
        if (p == -1) {
            return value;
        }
        int max = value.length();
        StringBuffer sb = new StringBuffer(max);
        int i = 0;		/* Index of last character we copied */
        while (true) {
            if (p > i) {
		/* Copy in anything before the special stuff */
                sb.append(value.substring(i, p));
                i = p;
            }
            int pe = value.indexOf('}', p + 2);
            if (pe == -1) {
		/* No matching '}' found, just add in as normal text */
                sb.append(value.substring(p, max));
                break;
            }
            String prop = value.substring(p + 2, pe);
            if (prop.equals("/")) {
                sb.append(File.separatorChar);
            } else {
                try {
                    String val = prop.length() == 0 ? null
                            : System.getProperty(prop);
                    if (val != null) {
                        sb.append(val);
                    } else {
                        oops("problem expanding system property '" + prop + "'",
                                lineno, override);
                    }
                } catch (SecurityException e) {
                    oops("problem expanding system property '" + prop + "'",
                            lineno, override, e);
                }
            }
            i = pe + 1;
            p = value.indexOf("${", i);
            if (p == -1) {
		/* No more to expand -- copy in any extra. */
                if (i < max) {
                    sb.append(value.substring(i, max));
                }
		/* Break out of loop */
                break;
            }
        }
        return sb.toString();
    }

    /**
     * Throws a ConfigurationException for an error described by the what argument, and for the
     * specified line number and override.
     */
    private void oops(String what, int lineno, int override)
            throws ConfigurationException {
        oops(what, lineno, override, null);
    }

    /**
     * Throws a ConfigurationException for an error described by the what argument, for the
     * specified line number and override, and caused by the specified evaluation exception, which
     * may be null.
     */
    private void oops(String what, int lineno, int override, Throwable t)
            throws ConfigurationException {
        ErrorDescriptor error = new ErrorDescriptor(lineno, override,
                what, location, t);
        throwConfigurationException(
                new ConfigurationException(error.toString(), t),
                Collections.singletonList(error));
        throw new AssertionError("throwConfigurationException must throw"
                + " an exception");
    }

    /**
     * Calls throwConfigurationException with a default NoSuchEntryException.
     */
    private void oopsNoSuchEntry(String what) throws ConfigurationException {
        ErrorDescriptor error = new ErrorDescriptor(0, 0, what, location);
        throwConfigurationException(
                new NoSuchEntryException(error.toString()),
                Collections.singletonList(error));
        throw new AssertionError("throwConfigurationException must throw"
                + " an exception");
    }

    /**
     * Returns a string representation of this object.
     */
    public String toString() {
        return "ConfigurationFile@" +
                Integer.toHexString(hashCode()) +
                (location == null ? "" : "{" + location + "}");
    }

    /**
     * Class used to represent a syntax error encountered when parsing a configuration source or a
     * problem encountered when attempting to return an existing entry or the type of an existing
     * entry.
     */
    public static class ErrorDescriptor implements Serializable {

        private static final long serialVersionUID = 1L;
        /**
         * line number where this syntax error occurred
         *
         * @serial
         */
        private final int lineno;
        /**
         * override where this syntax error occurred
         *
         * @serial
         */
        private final int override;
        /**
         * textual description of the problem encountered
         *
         * @serial
         */
        private final String description;
        /**
         * configuration source location name
         *
         * @serial
         */
        private final String locationName;
        /**
         * exception associated with this error
         *
         * @serial
         */
        private final Throwable t;

        /**
         * Creates a new error descriptor.
         *
         * @param lineno       line number of the configuration source where this problem was found
         *                     or <code>0</code> if this problem is not associated with a line
         *                     number
         * @param override     the override sequence number or <code>0</code> if the problem was not
         *                     found in an override
         * @param description  a description of the problem; this parameter cannot be
         *                     <code>null</code>
         * @param locationName the name of the configuration source location or <code>null</code> if
         *                     location information is not available
         * @param t            exception associated with this error or <code>null</code> if there is
         *                     no exception related to the error; <code>t</code> cannot be an
         *                     instance of <code>java.lang.Error</code>
         * @throws IllegalArgumentException if <code>lineno</code> < <code>0</code>,
         *                                  <code>override</code> < <code>0</code>,
         *                                  <code>description</code> is <code>null</code>, or
         *                                  <code>t</code> is an instance of <code>java.lang.Error</code>
         */
        public ErrorDescriptor(int lineno,
                               int override,
                               String description,
                               String locationName,
                               Throwable t) {
            if (lineno < 0)
                throw new IllegalArgumentException("line number must be"
                        + " greater than 0");
            if (override < 0)
                throw new IllegalArgumentException("override argument must be"
                        + " greater than 0");
            if (description == null)
                throw new IllegalArgumentException("description of the error"
                        + " cannot be null");
            if (t instanceof Error)
                throw new IllegalArgumentException("t cannot be an instance of"
                        + " java.lang.Error");
            this.lineno = lineno;
            this.override = override;
            this.description = description;
            this.locationName = locationName;
            this.t = t;
        }

        /**
         * Creates a new error descriptor.
         *
         * @param lineno       line number of the configuration source where this problem was found
         *                     or <code>0</code> if this problem is not associated with a line
         *                     number
         * @param override     the override sequence number or <code>0</code> if the problem was not
         *                     found in an override
         * @param description  a description of the problem; this parameter cannot be
         *                     <code>null</code>
         * @param locationName the name of the configuration source location or <code>null</code> if
         *                     location information is not available
         * @throws IllegalArgumentException if <code>lineno</code> < <code>0</code>,
         *                                  <code>override</code> < <code>0</code>, or
         *                                  <code>description</code> is <code>null</code>
         */
        public ErrorDescriptor(int lineno,
                               int override,
                               String description,
                               String locationName) {
            this(lineno, override, description, locationName, null);
        }

        /**
         * Creates a new error descriptor.
         *
         * @param lineno      line number of the configuration source where this problem was found
         *                    or <code>0</code> if this problem is not associated with a line
         *                    number
         * @param override    the override sequence number or <code>0</code> if the problem was not
         *                    found in an override
         * @param description a description of the problem; this parameter cannot be
         *                    <code>null</code>
         * @throws IllegalArgumentException if <code>lineno</code> < <code>0</code>,
         *                                  <code>override</code> < <code>0</code>, or
         *                                  <code>description</code> is <code>null</code>
         */
        public ErrorDescriptor(int lineno, int override, String description) {
            this(lineno, override, description, null);
        }

        /**
         * Returns the line number in the configuration source where the entry with an error can be
         * found or <code>0</code> if this error descriptor is not associated with a line number.
         *
         * @return the line number in the configuration source where the entry with an error can be
         * found or <code>0</code> if this error descriptor is not associated with a line number
         */
        public int getLineNumber() {
            return lineno;
        }

        /**
         * Returns the override sequence number where this error occurred or <code>0</code> if the
         * error did not occur in an override. For example, if the error occurred in the second
         * override specified, the method would return <code>2</code>.
         *
         * @return the override where this error occurred or <code>0</code> if this error did not
         * occur in an override
         */
        public int getOverride() {
            return override;
        }

        /**
         * Returns a textual description of the error encountered.
         *
         * @return a description of the problem
         */
        public String getDescription() {
            return description;
        }

        /**
         * Returns the name of the configuration source location or <code> null</code> if location
         * information is not available.
         *
         * @return the name of the configuration source location where this error occurred or
         * <code>null</code> if location information is not available.
         */
        public String getLocationName() {
            return locationName;
        }

        /**
         * Returns the exception associated with this error or <code>null </code> if there is no
         * exception associated with this error.
         *
         * @return the exception associated with this error or null if there is no exception
         * associated with this error.
         */
        public Throwable getCause() {
            return t;
        }

        /**
         * Returns a string representation of this error.
         *
         * @return the string representation of this error
         */
        public String toString() {
            StringBuffer buffer = new StringBuffer();
            if (override > 0) {
                buffer.append("Override " + override + ": ");
                if (lineno > 0)
                    buffer.append("Line " + lineno + ": ");
            } else {
                if (locationName == null && lineno > 0) {
                    buffer.append("Line " + lineno + ": ");
                } else if (locationName != null) {
                    buffer.append(locationName + ":");
                    if (lineno > 0)
                        buffer.append(lineno + ": ");
                }
            }
            buffer.append(description);
            return buffer.toString();
        }

        /**
         * Verifies that the deserialized field values for this error descriptor are valid.
         */
        private void readObject(ObjectInputStream in)
                throws IOException, ClassNotFoundException {
            in.defaultReadObject();
            if (lineno < 0)
                throw new InvalidObjectException("line number must be"
                        + " greater than 0");
            if (override < 0)
                throw new InvalidObjectException("override argument must be"
                        + " greater than 0");
            if (description == null)
                throw new InvalidObjectException("description of the error"
                        + " cannot be null");
            if (t instanceof Error)
                throw new InvalidObjectException("t cannot be an instance of"
                        + " java.lang.Error");
        }

        /**
         * Throws InvalidObjectException, since a descriptor is always required.
         */
        private void readObjectNoData() throws ObjectStreamException {
            throw new InvalidObjectException("no data");
        }
    }
}
