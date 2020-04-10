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

package net.jini.activation;

import com.gigaspaces.logger.LogUtils;
import com.sun.jini.action.GetBooleanAction;

import net.jini.constraint.BasicMethodConstraints;
import net.jini.core.constraint.InvocationConstraints;
import net.jini.core.constraint.MethodConstraints;
import net.jini.core.constraint.RemoteMethodControl;
import net.jini.io.UnsupportedConstraintException;
import net.jini.security.Security;
import net.jini.security.proxytrust.ProxyTrustIterator;
import net.jini.security.proxytrust.TrustEquivalence;

import java.io.IOException;
import java.io.InvalidObjectException;
import java.io.ObjectInputStream;
import java.io.Serializable;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.lang.reflect.Proxy;
import java.rmi.ConnectException;
import java.rmi.ConnectIOException;
import java.rmi.MarshalException;
import java.rmi.NoSuchObjectException;
import java.rmi.Remote;
import java.rmi.RemoteException;
import java.rmi.ServerError;
import java.rmi.ServerException;
import java.rmi.UnknownHostException;
import java.rmi.activation.ActivateFailedException;
import java.rmi.activation.ActivationException;
import java.rmi.activation.ActivationID;
import java.rmi.activation.UnknownObjectException;
import java.security.AccessController;
import java.security.PrivilegedAction;
import java.util.NoSuchElementException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * An invocation handler for activatable remote objects.  If the client constraints of this
 * activatable invocation handler are not <code>null</code>, then the invocation handler's
 * underlying proxy (if any) must implement {@link RemoteMethodControl} or a remote invocation will
 * fail with an {@link UnsupportedConstraintException}.
 *
 * @author Sun Microsystems, Inc.
 * @since 2.0
 *
 *
 * This implementation recognizes the following system property: <ul>
 * <li><code>com.sun.jini.activation.enableActivateGrant</code> - This property is interpreted as a
 * <code>boolean</code> value (see {@link Boolean#getBoolean Boolean.getBoolean}). If
 * <code>true</code>, this implementation invokes {@link Security#grant(Class, Class)
 * Security.grant} as defined in the specification. </ul>
 *
 * <p>This implementation's {@link #invoke invoke} method throws {@link IllegalArgumentException} if
 * a remote invocation is to be made and the <code>proxy</code> argument is an instance of an
 * interface whose binary name is <code>javax.management.MBeanServerConnection</code> or any of the
 * names produced by the following procedure:
 *
 * <blockquote>
 *
 * For each resource named <code>com/sun/jini/proxy/resources/InvocationHandler.moreProhibitedProxyInterfaces</code>
 * that is visible to the system class loader, the contents of the resource are parsed as UTF-8 text
 * to produce a list of interface names.  The resource must contain a list of binary names of
 * interfaces, one per line.  Space and tab characters surrounding each name, as well as blank
 * lines, are ignored.  The comment character is <tt>'#'</tt>; on each line, all characters starting
 * with the first comment character are ignored.
 *
 * </blockquote>
 *
 * <p>This implementation uses the {@link Logger} named <code>net.jini.activation.ActivatableInvocationHandler</code>
 * to log information at the following levels:
 *
 * <table summary="Describes what is logged by ActivatableInvocationHandler at various logging
 * levels" border=1 cellpadding=5>
 *
 * <tr> <th> Level <th> Description
 *
 * <tr> <td> {@link Level#FINE FINE} <td> exception thrown from final attempt to communicate a
 * remote call
 *
 * <tr> <td> {@link Level#FINE FINE} <td> exception thrown activating the object
 *
 * <tr> <td> {@link Level#FINE HANDLED} <td> exception caught in attempt to communicate a remote
 * call
 *
 * </table>
 **/
public final class ActivatableInvocationHandler
        implements InvocationHandler, TrustEquivalence, Serializable {
    private static final long serialVersionUID = -428224070630550856L;

    /**
     * The number of times to retry a call, with varying degrees of reactivation in between.
     */
    private static final int MAX_RETRIES = 3;

    /**
     * logger
     */
    private static final Logger logger =
            LoggerFactory.getLogger("net.jini.activation.ActivatableInvocationHandler");

    /**
     * Constructor parameter classes for proxy classes.
     */
    private static final Class[] constructorArgs =
            new Class[]{InvocationHandler.class};

    /**
     * Flag to enable use of Security.grant.
     */
    private static final boolean enableGrant =
            ((Boolean) AccessController.doPrivileged(new GetBooleanAction(
                    "com.sun.jini.activation.enableActivateGrant")))
                    .booleanValue();

    /**
     * The activation identifier.
     *
     * @serial
     */
    private final ActivationID id;

    /**
     * The underlying proxy or <code>null</code>.
     *
     * @serial
     */
    private Remote uproxy;

    /**
     * The client constraints or <code>null</code>.
     *
     * @serial
     */
    private final MethodConstraints clientConstraints;

    /*
     * The getProxyTrustIterator method object.
     */
    private static final Method getPtiMethod;

    static {
        try {
            getPtiMethod = ActivatableInvocationHandler.class.
                    getDeclaredMethod("getProxyTrustIterator", new Class[0]);
        } catch (NoSuchMethodException nsme) {
            throw new AssertionError(nsme);
        }
    }

    /**
     * Creates an instance with the specified activation identifier, a possibly-<code>null</code>
     * underlying proxy, and <code>null</code> client constraints. If the underlying proxy
     * implements {@link RemoteMethodControl} and its constraints are not <code>null</code>, the
     * underlying proxy of this instance is a copy of that proxy with <code>null</code>
     * constraints.
     *
     * @param id              the activation identifier
     * @param underlyingProxy an underlying proxy, or <code>null</code>
     * @throws NullPointerException if <code>id</code> is <code>null</code>
     **/
    public ActivatableInvocationHandler(ActivationID id,
                                        Remote underlyingProxy) {
        if (id == null) {
            throw new NullPointerException("id is null");
        }
        this.id = id;
        this.uproxy = underlyingProxy;
        this.clientConstraints = null;

	/*
     * If underlying proxy's constraints are non-null,
	 * set underlying proxy to a copy of the underlying
	 * proxy with null constraints.
	 */
        if (uproxy instanceof RemoteMethodControl) {
            MethodConstraints uproxyConstraints =
                    ((RemoteMethodControl) uproxy).getConstraints();
            if (uproxyConstraints != null) {
                uproxy = (Remote) ((RemoteMethodControl) uproxy).
                        setConstraints(null);
            }
        }
    }

    /**
     * Returns true if the constraints on the underlying proxy (if it implements {@link
     * RemoteMethodControl}) are equivalent to the constraints of this invocation handler, or if the
     * underlying proxy does not implement RemoteMethodControl.
     */
    private boolean hasConsistentConstraints() {
        if (uproxy instanceof RemoteMethodControl) {
            MethodConstraints uproxyConstraints =
                    ((RemoteMethodControl) uproxy).getConstraints();
            return (clientConstraints == null ?
                    uproxyConstraints == null :
                    clientConstraints.equals(uproxyConstraints));
        } else {
            return true;
        }
    }

    /**
     * Creates an instance with the specified activation identifier, optional underlying proxy, and
     * client constraints.  This constructor assumes that the client constraints are equivalent to
     * the constraints on the underlying proxy.
     **/
    private ActivatableInvocationHandler(
            ActivationID id,
            Remote underlyingProxy,
            MethodConstraints clientConstraints) {
        this.id = id;
        this.uproxy = underlyingProxy;
        this.clientConstraints = clientConstraints;
    }

    /**
     * Returns the activation identifier supplied during construction of this invocation handler.
     *
     * @return the activation identifier
     */
    public ActivationID getActivationID() {
        return id;
    }

    /**
     * Returns the current value for the underlying proxy.
     *
     * @return the underlying proxy
     */
    public synchronized Object getCurrentProxy() {
        return uproxy;
    }

    /**
     * Processes a method invocation made on the encapsulating proxy instance, <code>proxy</code>,
     * and returns the result. This method is invoked when a method is invoked on a proxy instance
     * that this handler is associated with.
     *
     * <p>If the specified method is one of the following <code>java.lang.Object</code> methods, it
     * will be processed as follows: <p><ul> <li>{@link Object#equals equals}: returns
     * <code>true</code> if the argument is an instance of a dynamically generated {@link Proxy}
     * class that implements the same ordered set of interfaces as the specified proxy, and this
     * invocation handler is equal to the invocation handler of that parameter, and returns
     * <code>false</code> otherwise. <li>{@link Object#hashCode hashCode}: returns the hash code for
     * the proxy. <li>{@link Object#toString toString}: returns a string representation of the
     * specified <code>proxy</code> object. </ul>
     *
     * <p>If the specified method is {@link RemoteMethodControl#setConstraints
     * RemoteMethodControl.setConstraints}, then if <code>proxy</code> is an instance of a dynamic
     * proxy class containing this invocation handler, returns a new proxy containing a copy of this
     * invocation handler with the same activation identifier, the new specified client constraints
     * (<code>args[0]</code>), and the current underlying proxy, or if the current underlying proxy
     * implements {@link RemoteMethodControl}, a copy of that proxy with the new specified client
     * constraints.  An exception is thrown if <code>proxy</code> is not an instance of a dynamic
     * proxy class containing this invocation handler.
     *
     * <p>If the specified method is {@link RemoteMethodControl#getConstraints
     * RemoteMethodControl.getConstraints}, returns the client constraints.
     *
     * <p>If the specified method is {@link TrustEquivalence#checkTrustEquivalence
     * TrustEquivalence.checkTrustEquivalence}, returns <code>true</code> if the argument
     * (<code>args[0]</code>) is an instance of a dynamic proxy class (that is, a class generated by
     * {@link Proxy}) that implements the same interfaces as the specified proxy and calling the
     * {@link #checkTrustEquivalence checkTrustEquivalence} method of this invocation handler with
     * the invocation handler of that argument returns <code>true</code>, and returns
     * <code>false</code> otherwise.
     *
     * <p>For all other methods, a remote invocation is made as follows:
     *
     * <p>A single set of absolute constraints (if any) is used for the duration of the remote
     * invocation, including any activation that may occur.
     *
     * <p><ul> <li>If the underlying proxy is non-<code>null</code>, the method is invoked as
     * follows:
     *
     * <ul> <li>If the client constraints of this object are not <code>null</code> and the
     * underlying proxy does not implement {@link RemoteMethodControl} then an {@link
     * UnsupportedConstraintException} is thrown.
     *
     * <li>If <code>method</code>'s declaring class is not <code>public</code>, the underlying proxy
     * is an instance of the <code>method</code>'s declaring class, and the underlying proxy's class
     * is <code>public</code>, then a <code>public</code> method with the same name and parameter
     * types is obtained from the underlying proxy's class, and if such a method exists, that method
     * is reflectively invoked on the underlying proxy passing it the specified <code>args</code>
     * and the result is returned; otherwise if such a method doesn't exist an
     * <code>ActivateFailedException</code> is thrown with {@link NoSuchMethodException} as the
     * cause.
     *
     * <li>Otherwise, the original <code>method</code> is reflectively invoked on the underlying
     * proxy passing it the specified <code>args</code>. </ul>
     *
     * <p>If this reflective invocation throws an exception other than {@link
     * IllegalAccessException}, {@link IllegalArgumentException}, the <code>ActivateFailedException</code>
     * described above, or an {@link InvocationTargetException} containing {@link ConnectException},
     * {@link ConnectIOException}, {@link NoSuchObjectException}, or {@link UnknownHostException},
     * then if the exception an {@link InvocationTargetException} the contained exception is thrown
     * to the caller, otherwise the exception is thrown directly.
     *
     * <p><li>If the underlying proxy is <code>null</code> or if the reflective invocation does not
     * throw an exception to the caller as described above: <ul>
     *
     * <li>If permitted by some implementation-specific mechanism, dynamically grants permissions to
     * the class loader of the activation identifier's class by invoking {@link
     * Security#grant(Class, Class) Security.grant} passing the class of the proxy and the class of
     * the activation identifier.  If this invocation throws an {@link
     * UnsupportedOperationException}, the exception is ignored.
     *
     * <li>A new proxy is obtained by invoking the {@link ActivationID#activate activate} method on
     * the activation identifier, passing <code>false</code> as the argument.  That method must
     * return an instance of a dynamic {@link Proxy} class, with an invocation handler that is an
     * instance of this class, containing the same activation identifier.  If the returned proxy
     * does not meet this criteria, then an {@link ActivateFailedException} is thrown.  If the
     * <code>activate</code> call throws {@link ConnectException}, then a new
     * <code>ConnectException</code> is thrown with the original <code>ConnectException</code> as
     * the cause.  If the <code>activate</code> call throws {@link RemoteException}, then {@link
     * ConnectIOException} is thrown with the <code>RemoteException</code> as the cause.  If the
     * <code>activate</code> call throws {@link UnknownObjectException}, then {@link
     * NoSuchObjectException} is thrown with the <code>UnknownObjectException</code> as the cause.
     * Finally, if the <code>activate</code> call throws {@link ActivationException}, then {@link
     * ActivateFailedException} is thrown with the <code>ActivationException</code> as the cause.
     *
     * <li>If a valid, new proxy is returned by the <code>activate</code> call, the underlying proxy
     * of the new proxy is obtained from the new proxy's activatable invocation handler.  If the
     * obtained underlying proxy implements <code>RemoteMethodControl</code>, this invocation
     * handler's underlying proxy is set to a copy of the obtained underlying proxy with the client
     * constraints of this instance. Otherwise, this invocation handler's underlying proxy is set to
     * the obtained underlying proxy.
     *
     * <li>The reflective invocation is then retried (as above) on the new underlying proxy.
     * Activation and retry can occur up to three times. On subsequent attempts, <code>true</code>
     * will be passed to the activation identifier's <code>activate</code> method, if passing
     * <code>false</code> returned the same underlying proxy as before or if
     * <code>NoSuchObjectException</code> was thrown by the call to the underlying proxy.
     *
     * <li>If the final attempt at reflective invocation throws <code>IllegalAccessException</code>
     * or <code>IllegalArgumentException</code> an <code>ActivateFailedException</code> is thrown
     * with the original exception as the cause.  If this reflective invocation throws
     * <code>InvocationTargetException</code>, the contained target exception is thrown. </ul>
     * </ul>
     *
     * <p>The implementation of remote method invocation defined by this class preserves
     * at-most-once call semantics: the remote call either does not execute, partially executes, or
     * executes exactly once at the remote site. Note that for remote calls to activatable objects,
     * arguments may be marshalled more than once.
     *
     * <p>The semantics of this method are unspecified if the arguments could not have been produced
     * by an instance of some valid dynamic proxy class containing this invocation handler. This
     * method throws {@link IllegalArgumentException} if <code>proxy</code> is an instance of
     * <code>InvocationHandler</code> or, if a remote invocation is to be made, any of the
     * superinterfaces of <code>proxy</code>'s class have a method with the same name and parameter
     * types as <code>method</code> but that does not declare <code>RemoteException</code> or a
     * superclass of <code>RemoteException</code> in its <code>throws</code> clause (even if such a
     * method is not a member of any of the direct superinterfaces of <code>proxy</code>'s class
     * because of overriding).
     *
     * @throws Throwable {@inheritDoc}
     * @see java.lang.reflect.UndeclaredThrowableException
     **/
    public Object invoke(Object proxy, Method method, Object[] args)
            throws Throwable {
        Class decl = method.getDeclaringClass();
        if (proxy instanceof InvocationHandler) {
            throw new IllegalArgumentException(
                    "proxy cannot be an invocation handler");
        } else if (decl == Object.class) {
            return invokeObjectMethod(proxy, method, args);
        } else if (decl == RemoteMethodControl.class) {
            return invokeRemoteMethodControlMethod(proxy, method, args);
        } else if (decl == TrustEquivalence.class) {
            return invokeTrustEquivalenceMethod(proxy, method, args);
        } else {
            return invokeRemoteMethod(proxy, method, args);
        }
    }

    /**
     * Handles java.lang.Object methods.
     **/
    private Object invokeObjectMethod(Object proxy,
                                      Method method,
                                      Object[] args) {
        String name = method.getName();

        if (name.equals("hashCode")) {
            return new Integer(hashCode());

        } else if (name.equals("equals")) {
            Object obj = args[0];
            boolean b =
                    proxy == obj ||
                            (obj != null &&
                                    sameProxyClass(proxy, obj) &&
                                    equals(Proxy.getInvocationHandler(obj)));
            return Boolean.valueOf(b);

        } else if (name.equals("toString")) {
            return proxyToString(proxy);

        } else {
            throw new IllegalArgumentException(
                    "unexpected Object method: " + method);
        }
    }

    /**
     * Handles RemoteMethodControl methods.
     **/
    private Object invokeRemoteMethodControlMethod(Object proxy,
                                                   Method method,
                                                   Object[] args)
            throws Throwable {
        String name = method.getName();

        if (name.equals("setConstraints")) {
            if (Proxy.getInvocationHandler(proxy) != this) {
                throw new IllegalArgumentException("not proxy for this");
            }
            Remote newProxy;
            synchronized (this) {
                newProxy = uproxy;
            }
            MethodConstraints mc = (MethodConstraints) args[0];
            if (newProxy instanceof RemoteMethodControl) {
                newProxy = (Remote)
                        ((RemoteMethodControl) newProxy).setConstraints(mc);
            }
            Class proxyClass = proxy.getClass();
            return Proxy.newProxyInstance(
                    getProxyLoader(proxyClass),
                    proxyClass.getInterfaces(),
                    new ActivatableInvocationHandler(id, newProxy, mc));
            // IllegalArgumentException means proxy argument was bogus

        } else if (name.equals("getConstraints")) {
            return clientConstraints;

        } else {
            throw new AssertionError(method);
        }
    }

    /**
     * Handles TrustEquivalence methods.
     **/
    private Object invokeTrustEquivalenceMethod(Object proxy,
                                                Method method,
                                                Object[] args) {
        String name = method.getName();
        if (name.equals("checkTrustEquivalence")) {
            Object obj = args[0];
            boolean b =
                    proxy == obj ||
                            (obj != null &&
                                    sameProxyClass(proxy, obj) &&
                                    checkTrustEquivalence(Proxy.getInvocationHandler(obj)));
            return Boolean.valueOf(b);

        } else {
            throw new AssertionError(method);
        }
    }

    /**
     * Handles remote methods.
     **/
    private Object invokeRemoteMethod(Object proxy,
                                      Method method,
                                      Object[] args)
            throws Throwable {
    /*
	 * Make the relative constraints (if any) on the method absolute.
	 * If there are relative constraints, create new method constraints
	 * to set on the underlying proxy before invoking a method.
	 */
        MethodConstraints convertedConstraints = null;
        if (clientConstraints != null) {
            InvocationConstraints relativeConstraints =
                    clientConstraints.getConstraints(method);
            InvocationConstraints absoluteConstraints =
                    relativeConstraints.makeAbsolute();
            if (relativeConstraints != absoluteConstraints) {
                convertedConstraints =
                        new BasicMethodConstraints(absoluteConstraints);
            }
        }

        boolean force = false;
        Remote currProxy;
        Failure failure = null;
	
	/*
	 * Attempt object activation if underlying proxy is unknown.
	 */
        synchronized (this) {
            if (uproxy == null) {
                activate(force, proxy, method);
                force = true;
            }
            currProxy = uproxy;
        }

        for (int retries = MAX_RETRIES; --retries >= 0; ) {
            if (logger.isDebugEnabled()) {
                if (failure != null) {
                    LogUtils.throwing(logger, this.getClass(), "invokeRemoteMethod", failure.exception, format("outbound call", method));
                }
            }

            if ((clientConstraints != null) &&
                    !(currProxy instanceof RemoteMethodControl)) {
                throw new UnsupportedConstraintException(
                        "underlying proxy does not implement RemoteMethodControl");
            }

	    /*
	     * Set constraints on target proxy only if relative constraints
	     * were made absolute.
	     */
            Remote targetProxy =
                    (convertedConstraints == null ?
                            currProxy :
                            (Remote) ((RemoteMethodControl) currProxy).
                                    setConstraints(convertedConstraints));

	    /*
	     * Invoke method on target proxy.
	     */
            Object result = invokeMethod(targetProxy, method, args);
            if (result instanceof Failure) {
                failure = (Failure) result;
                if (!failure.retry || retries <= 0) {
                    break;
                }
            } else {
                return result;
            }

	    /*
	     * Reattempt activation.
	     */
            synchronized (this) {
                if (uproxy == null || currProxy.equals(uproxy)) {
                    activate(force, proxy, method);
                    if (currProxy.equals(uproxy) &&
                            (failure.exception instanceof NoSuchObjectException) &&
                            !force) {
                        activate(true, proxy, method);
                    }
                    force = true;
                } else {
                    force = false;
                }
                currProxy = uproxy;
            }
        }

        if (logger.isDebugEnabled()) {
            LogUtils.throwing(logger, this.getClass(), "invokeRemoteMethod", failure.exception, format("outbound call", method));
        }
        throw failure.exception;
    }

    private static String format(String text, Method method) {
        return text + " " + method.getDeclaringClass().getName() + "." + method.getName() + " throws";
    }

    /**
     * Holds information about the communication failure of a remote call attempt.
     **/
    private static class Failure {

        /**
         * exception representing the communication failure
         */
        final Throwable exception;

        /**
         * failure is safe to retry (and possibly worth retrying) after
         */
        final boolean retry;

        Failure(Throwable exception, boolean retry) {
            this.exception = exception;
            this.retry = retry;
        }
    }

    /**
     * Reflectively invokes the method on the supplied proxy and returns the result in a
     * <code>Result</code> object.  If a <code>RemoteException</code> is thrown as a result of the
     * remote invocation, then the result object contains a non-<code>null</code> exception and the
     * retry field indicates whether invocation retry is possible without violating "at-most-once"
     * call semantics.  If the result object contains a <code>null</code> exception, then the value
     * field is the return value of the remote invocation.
     **/
    private Object invokeMethod(Object proxy, Method m, Object[] args)
            throws Throwable {
        try {
            return invokeMethod0(proxy, m, args);

        } catch (NoSuchObjectException e) {
            return new Failure(e, true);
        } catch (ConnectException e) {
            return new Failure(e, true);
        } catch (UnknownHostException e) {
            return new Failure(e, true);
        } catch (ConnectIOException e) {
            return new Failure(e, true);
        } catch (MarshalException e) {
            return new Failure(e, false);
        } catch (ServerError e) {
            return new Failure(e, false);
        } catch (ServerException e) {
            return new Failure(e, false);
        } catch (RemoteException e) {
            synchronized (this) {
                if (proxy.equals(uproxy)) {
                    uproxy = null;
                }
            }
            return new Failure(e, e instanceof ActivateFailedException);
        } catch (Exception e) {
            return new Failure(e, false);
        }
    }

    /**
     * Reflectively invokes the method on the supplied <code>proxy</code> as follows:
     *
     * <p>If the <code>method</code>'s declaring class is not <code>public</code>, the proxy is an
     * instance of the <code>method</code>'s declaring class, and the proxy class is
     * <code>public</code>, a <code>public</code> method with the same name and parameter types is
     * obtained from the proxy class, and if such a method exists, that method is reflectively
     * invoked on the proxy passing it the specified <code>args</code> and the result is returned,
     * otherwise if such a method doesn't exist an <code>ActivateFailedException</code> is thrown
     * with {@link NoSuchMethodException} as the cause.
     *
     * <p>Otherwise, the original <code>method</code> is reflectively invoked on the proxy passing
     * it the specified <code>args</code> and the result is returned.
     *
     * <p>If the reflective invocation throws <code>IllegalAccessException</code> or
     * <code>IllegalArgumentException</code>, an <code>ActivateFailedException</code> exception is
     * thrown with the original exception as the cause. If the reflective invocation throws {@link
     * InvocationTargetException} the contained target exception is thrown.
     *
     * @param proxy a proxy
     * @param m     a method
     * @param arg   arguments
     * @return result of reflective invocation
     * @throws ActivateFailedException if the reflective invocation throws <code>IllegalAccessException</code>
     *                                 or <code>IllegalArgumentException</code>
     * @throws Throwable               if the reflective invocation throws <code>InvocationTargetException</code>,
     *                                 the contained target exception is thrown
     **/
    private static Object invokeMethod0(Object proxy, Method m, Object[] args)
            throws Throwable {
        Class mcl = m.getDeclaringClass();
        if (!Modifier.isPublic(mcl.getModifiers())) {
            Class cl = proxy.getClass();
            if (mcl.isAssignableFrom(cl) &&
                    Modifier.isPublic(cl.getModifiers())) {
                try {
                    m = cl.getMethod(m.getName(), m.getParameterTypes());
                } catch (NoSuchMethodException nsme) {
                    throw new ActivateFailedException("bad proxy").
                            initCause(nsme);
                }
            }
        }

        try {
            return m.invoke(proxy, args);
        } catch (IllegalAccessException e) {
            throw new ActivateFailedException("bad proxy").initCause(e);
        } catch (IllegalArgumentException e) {
            throw new ActivateFailedException("bad proxy").initCause(e);
        } catch (InvocationTargetException e) {
            throw e.getTargetException();
        }
    }

    /**
     * Returns a proxy trust iterator for an activatable object that is suitable for use by {@link
     * net.jini.security.proxytrust.ProxyTrustVerifier}.
     *
     * <p>The iterator produces the current underlying proxy on each iteration.  The iterator
     * produces up to three elements, but after the first element, iteration terminates unless the
     * exception set by a call to {@link ProxyTrustIterator#setException setException} on the
     * previous iteration is an instance of {@link ConnectException}, {@link ConnectIOException},
     * {@link NoSuchObjectException}, or {@link UnknownHostException}.
     *
     * <p>On each iteration, if the current underlying proxy is <code>null</code> or the same as the
     * underlying proxy produced by the previous iteration:
     *
     * <p>A new proxy is obtained by invoking the {@link ActivationID#activate activate} method on
     * the activation identifier, passing <code>false</code> as the argument.  That method must
     * return an instance of a dynamic {@link Proxy} class, with an invocation handler that is an
     * instance of this class, containing the same activation identifier.  If this activation throws
     * one of the following exceptions, the exception is thrown by the <code>next</code> method of
     * the iterator and the iteration terminates:
     *
     * <blockquote>If the proxy returned by the <code>activate</code> call does not meet the
     * criteria listed above, then an {@link ActivateFailedException} is thrown.  If the
     * <code>activate</code> call throws {@link RemoteException}, then {@link ConnectIOException} is
     * thrown with the <code>RemoteException</code> as the cause.  If the <code>activate</code> call
     * throws {@link UnknownHostException}, then {@link NoSuchObjectException} is thrown with the
     * <code>UnknownHostException</code> as the cause.  Finally, if the <code>activate</code> call
     * throws {@link ActivationException}, then {@link ActivateFailedException} is thrown with the
     * <code>ActivationException</code> as the cause. </blockquote>
     *
     * <p>If a valid, new proxy is returned by the <code>activate</code> call, the underlying proxy
     * of the new proxy is obtained from the new proxy's activatable invocation handler.  If the
     * obtained underlying proxy implements <code>RemoteMethodControl</code>, this invocation
     * handler's underlying proxy is set to a copy of the obtained underlying proxy with the client
     * constraints of this instance. Otherwise, this invocation handler's underlying proxy is set to
     * the obtained underlying proxy.
     *
     * <p>On the first call to the activation identifier's <code>activate</code> method,
     * <code>false</code> is passed as an argument; on subsequent calls <code>true</code> will be
     * passed, if passing <code>false</code> returned the same underlying proxy as before (when
     * compared using the <code>equals</code> method) or if the exception passed to
     * <code>setException</code> is an instance of <code>NoSuchObjectException</code>. If an
     * activation attempt results in an exception, that exception is thrown by the <code>next</code>
     * method of the iterator and iteration terminates.
     *
     * @return a proxy trust iterator suitable for use by <code>ProxyTrustVerifier</code>
     **/
    protected ProxyTrustIterator getProxyTrustIterator() {
        return new ProxyTrustIterator() {
            private int retries = MAX_RETRIES + 1;
            private boolean force = false;
            private Remote currProxy = null;
            private Exception fail = null;
            private RemoteException ex = null;
            private boolean advance = true;

            public synchronized boolean hasNext() {
                if (advance) {
                    advance = false;
                    if (--retries < 0) {
                    } else if (retries == MAX_RETRIES ||
                            ex instanceof NoSuchObjectException ||
                            ex instanceof ConnectException ||
                            ex instanceof UnknownHostException ||
                            ex instanceof ConnectIOException) {
                        try {
                            synchronized
                                    (ActivatableInvocationHandler.this) {
                                if (uproxy == null || uproxy.equals(currProxy)) {
                                    activate(force, null, getPtiMethod);
                                    if (uproxy.equals(currProxy) &&
                                            ex instanceof NoSuchObjectException &&
                                            !force) {
                                        activate(true, null, getPtiMethod);
                                    }
                                    force = true;
                                } else {
                                    force = false;
                                }
                                currProxy = uproxy;
                            }
                        } catch (Exception e) {
                            fail = e;
                            retries = 0;
                        }
                    } else {
                        retries = -1;
                        if (!(ex == null ||
                                ex instanceof MarshalException ||
                                ex instanceof ServerException ||
                                ex instanceof ServerError)) {
                            synchronized (ActivatableInvocationHandler.this) {
                                if (currProxy.equals(uproxy)) {
                                    uproxy = null;
                                }
                            }
                        }
                    }
                }
                return retries >= 0;
            }

            public synchronized Object next() throws RemoteException {
                if (!hasNext()) {
                    throw new NoSuchElementException();
                }
                advance = true;
                if (fail == null) {
                    return currProxy;
                } else if (fail instanceof RemoteException) {
                    throw (RemoteException) fail;
                } else {
                    throw (RuntimeException) fail;
                }
            }

            public synchronized void setException(RemoteException e) {
                if (e == null) {
                    throw new NullPointerException("exception is null");
                } else if (retries > MAX_RETRIES || !advance || fail != null) {
                    throw new IllegalStateException();
                }
                ex = e;
            }
        };
    }

    /**
     * Returns <code>true</code> if the specified object (which is not yet known to be trusted) is
     * equivalent in trust, content, and function to this known trusted object, and
     * <code>false</code> otherwise.
     *
     * <p><code>ActivatableInvocationHandler</code> implements this method as follows:
     *
     * <p>This method returns <code>true</code> if and only if the following conditions are met:
     * <ul> <li> The specified object has the same class as this object. <li> This object's
     * activation identifier implements {@link TrustEquivalence}. <li> Invoking the
     * <code>checkTrustEquivalence</code> method on this object's activation identifier passing the
     * specified object's activation identifier returns <code>true</code>. <li> The client
     * constraints in the specified object are equal to the ones in this object. </ul>
     *
     * <p>The underlying proxy of the specified object is set to <code>null</code> if this method
     * returns <code>true</code> and any of the following conditions are met: <ul> <li> This
     * object's underlying proxy is <code>null</code>. <li> This object's underlying proxy is not an
     * instance of {@link TrustEquivalence}. <li> Invoking the <code>checkTrustEquivalence</code>
     * method on this object's underlying proxy, passing the underlying proxy of the specified
     * object, returns <code>false</code>. </ul>
     **/
    public boolean checkTrustEquivalence(Object obj) {
        if (this == obj) {
            return true;
        } else if (!(obj instanceof ActivatableInvocationHandler) ||
                !(id instanceof TrustEquivalence)) {
            return false;
        }
        ActivatableInvocationHandler other =
                (ActivatableInvocationHandler) obj;
        if (!((TrustEquivalence) id).checkTrustEquivalence(other.id) ||
                (clientConstraints != other.clientConstraints &&
                        (clientConstraints == null ||
                                !clientConstraints.equals(other.clientConstraints)))) {
            return false;
        }
        Remote currProxy;
        synchronized (this) {
            currProxy = uproxy;
        }
        Remote otherProxy;
        synchronized (other) {
            otherProxy = other.uproxy;
        }
        if (currProxy == null || !(currProxy instanceof TrustEquivalence) ||
                !((TrustEquivalence) currProxy).checkTrustEquivalence(otherProxy)) {
            synchronized (other) {
                other.uproxy = null;
            }
        }
        return true;
    }

    /**
     * Activate the object (see activate0).
     **/
    private void activate(boolean force, Object proxy, Method method)
            throws Exception {
        assert Thread.holdsLock(this);

        try {
            activate0(force, proxy);
        } catch (Exception e) {
            if (logger.isDebugEnabled()) {
                LogUtils.throwing(logger, this.getClass(), "activate", e, format("activating object for call", method));
            }
            throw e;
        }
    }

    /**
     * Activate the object and update the underlying proxy. The force argument is passed on to the
     * activate method of the activation identifier.  If this method does not throw an exception,
     * the value of uproxy is updated to the value returned by calling the id's activate method.
     * Note: The caller should be synchronized on "this" while calling this method.
     *
     * @param force boolean to pass to activation id's activate method
     * @param proxy outer proxy from which dynamic grants are inherited, or null
     **/
    private void activate0(boolean force, Object proxy)
            throws RemoteException {
        assert Thread.holdsLock(this);

        uproxy = null;
        try {
            if (proxy != null && enableGrant) {
                try {
                    Security.grant(proxy.getClass(), id.getClass());
                } catch (UnsupportedOperationException uoe) {
                }
            }
            Object newProxy = id.activate(force);
            if (!Proxy.isProxyClass(newProxy.getClass())) {
                throw new ActivateFailedException("invalid proxy");
            }

            InvocationHandler obj = Proxy.getInvocationHandler(newProxy);
            if (!(obj instanceof ActivatableInvocationHandler)) {
                throw new ActivateFailedException("invalid proxy handler");
            }
            ActivatableInvocationHandler handler =
                    (ActivatableInvocationHandler) obj;
            if (!id.equals(handler.id)) {
                throw new ActivateFailedException("unexpected activation id");
            }

            Remote newUproxy = handler.uproxy;
            if (newUproxy == null) {
                throw new ActivateFailedException("null underlying proxy");
            } else if (newUproxy instanceof RemoteMethodControl) {
                newUproxy = (Remote) ((RemoteMethodControl) newUproxy).
                        setConstraints(clientConstraints);
            }
            uproxy = newUproxy;

        } catch (ConnectException e) {
            throw new ConnectException("activation failed", e);
        } catch (RemoteException e) {
            throw new ConnectIOException("activation failed", e);
        } catch (UnknownObjectException e) {
            throw new NoSuchObjectException("object not registered");
        } catch (ActivationException e) {
            throw new ActivateFailedException("activation failed", e);
        }
    }

    /**
     * Compares the specified object with this <code>ActivatableInvocationHandler</code> for
     * equality.
     *
     * <p> This method returns <code>true</code> if and only if the specified object has the same
     * class as this object, and the activation identifier and client constraints in the specified
     * object are equal to the ones in this object.
     **/
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        } else if (!(obj instanceof ActivatableInvocationHandler)) {
            return false;
        }
        ActivatableInvocationHandler other =
                (ActivatableInvocationHandler) obj;
        return (id.equals(other.id) &&
                (clientConstraints == other.clientConstraints ||
                        (clientConstraints != null &&
                                clientConstraints.equals(other.clientConstraints))));
    }

    /**
     * Returns a hash code value for this object.
     */
    public int hashCode() {
        return id.hashCode();
    }

    /**
     * Returns a string representation of this object.
     */
    public String toString() {
        return "ActivatableInvocationHandler[" + id + ", " + uproxy + "]";
    }

    /**
     * Returns a string representation for a proxy that uses this invocation handler.
     */
    private String proxyToString(Object proxy) {
        Class[] interfaces = proxy.getClass().getInterfaces();
        Class iface = null;
        for (int i = interfaces.length; --i >= 0; iface = interfaces[i]) {
            if (interfaces[i] == RemoteMethodControl.class) {
                break;
            }
        }
        if (iface == null) {
            return "Proxy[" + this + "]";
        }
        String n = iface.getName();
        int dot = n.lastIndexOf('.');
        if (dot >= 0) {
            n = n.substring(dot + 1);
        }
        return "Proxy[" + n + "," + this + "]";
    }

    /**
     * Verifies that the activation identifier is not <code>null</code>, and that the constraints on
     * this invocation handler and the underlying proxy are consistent.
     *
     * @throws InvalidObjectException if the activation identifier is <code>null</code>, or if the
     *                                underlying proxy implements {@link RemoteMethodControl} and
     *                                the constraints on the underlying proxy are not equivalent to
     *                                this invocation handler's constraints
     **/
    private void readObject(ObjectInputStream s)
            throws IOException, ClassNotFoundException {
        s.defaultReadObject();
        if (id == null) {
            throw new InvalidObjectException("id is null");
        } else {
            if (!hasConsistentConstraints()) {
                throw new InvalidObjectException(
                        "inconsistent constraints between underlying proxy and invocation handler");
            }
        }
    }

    /**
     * Returns the class loader for the specified proxy class.
     */
    private static ClassLoader getProxyLoader(final Class proxyClass) {
        return (ClassLoader)
                AccessController.doPrivileged(new PrivilegedAction() {
                    public Object run() {
                        return proxyClass.getClassLoader();
                    }
                });
    }

    /**
     * Returns true if proxy2 is a generated Proxy (proxy1 is assumed to be one) and the classes of
     * both proxies implement the same ordered list of interfaces, and returns false otherwise.
     */
    public static boolean sameProxyClass(Object proxy1, Object proxy2) {
        return (proxy1.getClass() == proxy2.getClass() ||
                (Proxy.isProxyClass(proxy2.getClass()) &&
                        equalInterfaces(proxy1, proxy2)));
    }

    /**
     * Returns true if the interfaces implemented by obj1's class are the same (and in the same
     * order) as obj2's class.
     */
    public static boolean equalInterfaces(Object obj1, Object obj2) {
        Class[] intf1 = obj1.getClass().getInterfaces();
        Class[] intf2 = obj2.getClass().getInterfaces();
        if (intf1.length != intf2.length) {
            return false;
        } else {
            for (int i = 0; i < intf1.length; i++) {
                if (intf1[i] != intf2[i]) {
                    return false;
                }
            }
            return true;
        }
    }
}
