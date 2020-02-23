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

package net.jini.security.proxytrust;

import com.sun.jini.logging.Levels;

import net.jini.core.constraint.MethodConstraints;
import net.jini.core.constraint.RemoteMethodControl;
import net.jini.io.MarshalInputStream;
import net.jini.io.ObjectStreamContext;
import net.jini.security.SecurityContext;
import net.jini.security.TrustVerifier;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.lang.reflect.Proxy;
import java.net.MalformedURLException;
import java.rmi.RemoteException;
import java.rmi.UnmarshalException;
import java.rmi.server.RMIClassLoader;
import java.security.AccessController;
import java.security.PrivilegedAction;
import java.security.PrivilegedActionException;
import java.security.PrivilegedExceptionAction;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Trust verifier for service proxies that use dynamically downloaded code. This verifier uses a
 * recursive algorithm to obtain one or more bootstrap proxies, which must be objects that are
 * instances of both {@link ProxyTrust} and {@link RemoteMethodControl}. If a bootstrap proxy (or a
 * derivative of it) is known to be trusted, a remote call is made through it to obtain a trust
 * verifier for the original service proxy. This class is intended to be specified in a resource to
 * configure the operation of {@link net.jini.security.Security#verifyObjectTrust
 * Security.verifyObjectTrust}.
 *
 * com.sun.jini.impl - This implementation uses the {@link Logger} named
 * <code>net.jini.security.trust</code> to log information at the following levels: <table
 * summary="Describes what is logged by ProxyTrustVerifier to the trust logger at various logging
 * levels" border=1 cellpadding=5> <tr> <th>Level</th> <th>Description</th> </tr> <tr> <td>{@link
 * Level#FINE FINE}</td> <td>no verifier is obtained from a {@link ProxyTrustIterator}</td>
 * </tr> <tr> <td>{@link Levels#HANDLED HANDLED}</td> <td><code>RemoteException</code> being passed
 * to {@link ProxyTrustIterator#setException ProxyTrustIterator.setException}</td> </tr> <tr>
 * <td>{@link Level#FINE FINE}</td> <td>{@link ProxyTrust#getProxyVerifier
 * ProxyTrust.getProxyVerifier} remote call returns a trust verifier</td> </tr> <tr> <td>{@link
 * Level#FINER FINER}</td> <td>an object with a <code>getProxyTrustIterator</code> method is
 * encountered</td> </tr> <tr> <td>{@link Level#FINER FINER}</td> <td>each object produced by a
 * {@link ProxyTrustIterator} and each derivative bootstrap proxy</td> </tr> </table>
 *
 * @author Sun Microsystems, Inc.
 * @since 2.0
 */
@com.gigaspaces.api.InternalApi
public class ProxyTrustVerifier implements TrustVerifier {

    private static final Logger logger =
            Logger.getLogger("net.jini.security.trust");

    /**
     * Thread-local state containing object to skip, if any
     */
    private static final ThreadLocal state = new ThreadLocal();
    /**
     * ProxyTrust.getProxyVerifier
     */
    private static Method gpvMethod;

    static {
        try {
            gpvMethod =
                    ProxyTrust.class.getMethod("getProxyVerifier", new Class[0]);
        } catch (Exception e) {
            throw new ExceptionInInitializerError(e);
        }
    }

    /**
     * Creates an instance.
     */
    public ProxyTrustVerifier() {
    }

    /**
     * Returns <code>true</code> if the specified object is known to be trusted to correctly
     * implement its contract; returns <code>false</code> otherwise. <p> This method returns
     * <code>false</code> if the caller context collection of the specified trust verifier context
     * does not contain a {@link MethodConstraints} instance with non-empty constraints for the
     * {@link ProxyTrust#getProxyVerifier ProxyTrust.getProxyVerifier} method, or if a
     * <code>TrustVerifier</code> cannot be obtained from the specified object using the steps
     * described below. Otherwise a <code>TrustVerifier</code> is obtained, its {@link
     * TrustVerifier#isTrustedObject isTrustedObject} method is called with the same arguments
     * passed to this method, and the result of that call is returned by this method; any exception
     * thrown by that call is thrown by this method. If a verifier cannot be obtained but one or
     * more of the intermediate operations involved in attempting to obtain one throws a
     * <code>RemoteException</code>, the last such <code>RemoteException</code> is thrown by this
     * method (rather than this method returning <code>false</code>). If any intermediate operation
     * throws a <code>SecurityException</code> exception, that exception is immediately thrown by
     * this method. <p> A verifier is obtained from a candidate object as follows. <ul> <li> If
     * either the candidate object's class has a non-<code>static</code> member method with
     * signature:
     * <pre>ProxyTrustIterator getProxyTrustIterator();</pre>
     * or the candidate object is an instance of a dynamically generated {@link Proxy} class and the
     * contained invocation handler's class has such a member method, then the
     * <code>getProxyTrustIterator</code> method is called (on the candidate object or its
     * invocation handler). For each object produced by the {@link ProxyTrustIterator#next next}
     * method of the returned iterator, the following substeps are used, until either a verifier is
     * obtained or the iteration terminates. If no verifier can be obtained from any object produced
     * by the iterator, then there is no verifier for the candidate object. For any given object
     * produced by the iterator, if a verifier cannot be obtained from the object but an
     * intermediate operation involved in attempting to obtain a verifier throws a
     * <code>RemoteException</code>, that exception is passed to the {@link
     * ProxyTrustIterator#setException setException} method of the iterator, and the iteration
     * continues. <p> The <code>getProxyTrustIterator</code> method and the
     * <code>ProxyTrustIterator</code> methods are all invoked in a restricted security context. If
     * the specified trust verifier context contains an {@link UntrustedObjectSecurityContext}
     * instance, then the security context returned by its {@link UntrustedObjectSecurityContext#getContext
     * getContext} method is used. Otherwise, the security context used is equivalent to the current
     * security context (as returned by {@link net.jini.security.Security#getContext
     * Security.getContext}) with an additional protection domain combined into the access control
     * context that contains an empty {@link java.security.CodeSource} (<code>null</code> location
     * and certificates), <code>null</code> permissions, <code>null</code> class loader, and
     * <code>null</code> principals. <ul> <li>If the object is an instance of both {@link
     * ProxyTrust} and {@link RemoteMethodControl} (that is, if the object is a bootstrap proxy), it
     * is verified for trust by calling the specified context's {@link
     * net.jini.security.TrustVerifier.Context#isTrustedObject isTrustedObject} method with the
     * object. If <code>isTrustedObject</code> returns <code>true</code>, then the object's {@link
     * ProxyTrust#getProxyVerifier getProxyVerifier} method is called, using as the client
     * constraints for the remote call the first <code>MethodConstraints</code> instance obtained
     * from the caller context collection (of the specified trust verifier context) that has
     * non-empty constraints for that <code>getProxyVerifier</code> method. The verifier returned by
     * that remote call is the verifier for the original top-level object, and the entire search
     * stops. If <code>isTrustedObject</code> returns <code>false</code>, but a verifier can be
     * obtained from a trusted derivative bootstrap proxy as described below, then that verifier is
     * the verifier for the original top-level object, and the entire search stops. Otherwise, no
     * verifier can be obtained from the object, and the iteration continues. <li>If the object is
     * not a <code>ProxyTrust</code> instance, it is in turn treated as a new candidate object, and
     * the complete set of steps for a candidate object are used recursively to obtain a verifier
     * from it. If a verifier can be obtained from it, that verifier is the verifier for the
     * original top-level object, and the entire search stops. If a verifier cannot be obtained from
     * it, the iteration continues. </ul> <li>If the candidate object is the original top-level
     * object and it is an instance of both <code>ProxyTrust</code> and
     * <code>RemoteMethodControl</code> (that is, if the original top-level object is itself a
     * bootstrap proxy), and a verifier can be obtained from a trusted derivative bootstrap proxy as
     * described below, that verifier is the verifier for the original top-level object, and the
     * entire search stops. </ul> Given a bootstrap proxy, a verifier can be obtained from a trusted
     * derivative bootstrap proxy as follows. A derivative can be produced from the bootstrap proxy
     * if all of the following conditions are satisfied: the bootstrap proxy was not itself produced
     * (either from an iteration or as a derivative) by the latest active invocation of
     * <code>ProxyTrustVerifier</code> (not including the current one) in this thread; the bootstrap
     * proxy is an instance of a dynamically generated <code>Proxy</code> class; neither the proxy's
     * class nor the invocation handler's class has an appropriate <code>getProxyTrustIterator</code>
     * method; the class loader of the proxy's class is the proper Java(TM) RMI class loader (as
     * defined below) for its parent class loader and the class's codebase (as produced by {@link
     * RMIClassLoader#getClassAnnotation RMIClassLoader.getClassAnnotation}); and both
     * <code>ProxyTrust</code> and <code>RemoteMethodControl</code> are loadable by the parent class
     * loader. The derivative that is produced is an instance of a dynamically generated
     * <code>Proxy</code> class defined by the parent class loader that implements both
     * <code>ProxyTrust</code> and <code>RemoteMethodControl</code> and contains the same invocation
     * handler as the bootstrap proxy. The derivative is a trusted derivative bootstrap proxy if
     * calling the specified context's <code>isTrustedObject</code> method with the derivative
     * returns <code>true</code>. If a trusted derivative bootstrap proxy can be produced, its
     * {@link ProxyTrust#getProxyVerifier getProxyVerifier} method is called, using as the client
     * constraints for the remote call the first <code>MethodConstraints</code> instance obtained
     * from the caller context collection (of the specified trust verifier context) that has
     * non-empty constraints for that <code>getProxyVerifier</code> method. The returned verifier is
     * used as is, if the class loader of the returned verifier's class is equal to the class loader
     * of the original bootstrap proxy's class, or if, in generating a serialization of the
     * verifier, no class passed to {@link ObjectOutputStream#annotateClass
     * ObjectOutputStream.annotateClass} or {@link ObjectOutputStream#annotateProxyClass
     * ObjectOutputStream.annotateProxyClass} has a class loader not equal to the class loader of
     * the original bootstrap proxy's class but has a codebase that is equal to the codebase of the
     * original bootstrap proxy's class. Otherwise, the verifier is remarshalled in a manner
     * equivalent to creating a {@link net.jini.io.MarshalledInstance} with the verifier and then
     * calling the {@link net.jini.io.MarshalledInstance#get(ClassLoader, boolean, ClassLoader,
     * Collection) get} method of that object with the class loader of the original bootstrap
     * proxy's class as the default loader, with no codebase integrity verification and with an
     * empty context collection, and the remarshalled verifier is used instead. If an {@link
     * IOException} or {@link ClassNotFoundException} is thrown by this remarshalling, the exception
     * is wrapped in an {@link UnmarshalException} and the resulting exception is treated as if it
     * had been thrown by the remote call that returned the verifier. <p> A class loader of a class
     * is the proper Java RMI class loader for its parent class loader and the class's codebase if
     * the class loader is not <code>null</code>, the codebase for the class is a non-empty string,
     * and calling {@link RMIClassLoader#getClassLoader RMIClassLoader.getClassLoader} with that
     * codebase, with the thread's context class loader set to the parent class loader, returns the
     * class loader of the class.
     *
     * @throws NullPointerException {@inheritDoc}
     * @throws SecurityException    {@inheritDoc}
     */
    public boolean isTrustedObject(Object obj, TrustVerifier.Context ctx)
            throws RemoteException {
        if (obj == null || ctx == null) {
            throw new NullPointerException();
        }
        MethodConstraints mc = null;
        UntrustedObjectSecurityContext uosc = null;
        for (Iterator iter = ctx.getCallerContext().iterator();
             (mc == null || uosc == null) && iter.hasNext(); ) {
            Object elt = iter.next();
            if (mc == null && elt instanceof MethodConstraints) {
                MethodConstraints emc = (MethodConstraints) elt;
                if (!emc.getConstraints(gpvMethod).isEmpty()) {
                    mc = emc;
                }
            } else if (uosc == null &&
                    elt instanceof UntrustedObjectSecurityContext) {
                uosc = (UntrustedObjectSecurityContext) elt;
            }
        }
        if (mc == null) {
            return false;
        } else if (uosc == null) {
            uosc = new BasicUntrustedObjectSecurityContext(null);
        }
        TrustVerifier verifier = getVerifier(obj, ctx, mc, uosc);
        return (verifier != null &&
                verifier.isTrustedObject(obj, ctx));
    }

    /**
     * Recursively tries to obtain a verifier from the remote server.
     */
    private static TrustVerifier getVerifier(
            Object obj,
            TrustVerifier.Context ctx,
            MethodConstraints mc,
            UntrustedObjectSecurityContext uosc)
            throws RemoteException {
        Method m = getMethod(obj);
        if (m == null) {
            if (!Proxy.isProxyClass(obj.getClass())) {
                return null;
            }
            Object ih = Proxy.getInvocationHandler(obj);
            m = getMethod(ih);
            if (m != null) {
                obj = ih;
            } else if (obj instanceof ProxyTrust &&
                    obj instanceof RemoteMethodControl) {
                return getAltVerifier(obj, ctx, mc);
            } else {
                return null;
            }
        }
        logger.log(Level.FINER, "{0} has ProxyTrustIterator", obj);
        SecurityContext rsc = uosc.getContext();
        ProxyTrustIterator iter;
        try {
            iter = (ProxyTrustIterator) restrictedInvoke(m, obj, rsc);
        } catch (IllegalAccessException e) {
            throw new AssertionError(e);
        } catch (InvocationTargetException e) {
            Throwable t = e.getTargetException();
            if (t instanceof RuntimeException) {
                throw (RuntimeException) t;
            }
            throw (Error) t;
        }
        RemoteException lastEx = null;
        while (restrictedHasNext(iter, rsc)) {
            obj = null;
            try {
                obj = restrictedNext(iter, rsc);
                logger.log(Level.FINER, "ProxyTrustIterator produces {0}",
                        obj);
                if (!(obj instanceof ProxyTrust)) {
                    TrustVerifier verifier = getVerifier(obj, ctx, mc, uosc);
                    if (verifier != null) {
                        return verifier;
                    }
                } else if (obj instanceof RemoteMethodControl) {
                    if (isTrusted(obj, ctx)) {
                        obj = ((RemoteMethodControl) obj).setConstraints(mc);
                        TrustVerifier verifier =
                                ((ProxyTrust) obj).getProxyVerifier();
                        logger.log(Level.FINE, "verifier is {0}", verifier);
                        return verifier;
                    } else if (Proxy.isProxyClass(obj.getClass()) &&
                            getMethod(obj) == null &&
                            getMethod(Proxy.getInvocationHandler(obj)) ==
                                    null) {
                        TrustVerifier verifier = getAltVerifier(obj, ctx, mc);
                        if (verifier != null) {
                            return verifier;
                        }
                    }
                }
            } catch (RemoteException e) {
                lastEx = e;
                if (obj instanceof ProxyTrust) {
                    logger.log(Levels.HANDLED,
                            "setting ProxyTrustIterator exception", e);
                    restrictedSetException(iter, e, rsc);
                }
            }
        }
        if (lastEx != null) {
            throw lastEx;
        }
        logger.log(Level.FINE,
                "no verifier obtained from ProxyTrustIterator");
        return null;
    }

    /**
     * Calls m.invoke(obj, null) in context of rsc.
     */
    private static Object restrictedInvoke(final Method m,
                                           final Object obj,
                                           SecurityContext rsc)
            throws IllegalAccessException, InvocationTargetException {
        try {
            return AccessController.doPrivileged(rsc.wrap(
                    new PrivilegedExceptionAction() {
                        public Object run()
                                throws IllegalAccessException,
                                InvocationTargetException {
                            return m.invoke(obj, null);
                        }
                    }), rsc.getAccessControlContext());
        } catch (PrivilegedActionException pae) {
            Exception e = pae.getException();
            if (e instanceof InvocationTargetException) {
                throw (InvocationTargetException) e;
            } else {
                throw (IllegalAccessException) e;
            }
        }
    }

    /**
     * Calls iter.hasNext() in context of acc.
     */
    private static boolean restrictedHasNext(final ProxyTrustIterator iter,
                                             SecurityContext rsc) {
        return ((Boolean)
                AccessController.doPrivileged(rsc.wrap(
                        new PrivilegedAction() {
                            public Object run() {
                                return Boolean.valueOf(iter.hasNext());
                            }
                        }), rsc.getAccessControlContext())).booleanValue();
    }

    /**
     * Calls iter.next() in context of rsc.
     */
    private static Object restrictedNext(final ProxyTrustIterator iter,
                                         SecurityContext rsc)
            throws RemoteException {
        try {
            return AccessController.doPrivileged(rsc.wrap(
                    new PrivilegedExceptionAction() {
                        public Object run() throws RemoteException {
                            return iter.next();
                        }
                    }), rsc.getAccessControlContext());
        } catch (PrivilegedActionException e) {
            throw (RemoteException) e.getException();
        }
    }

    /**
     * Calls iter.setException(e) in context of rsc.
     */
    private static void restrictedSetException(final ProxyTrustIterator iter,
                                               final RemoteException e,
                                               SecurityContext rsc) {
        AccessController.doPrivileged(rsc.wrap(
                new PrivilegedAction() {
                    public Object run() {
                        iter.setException(e);
                        return null;
                    }
                }), rsc.getAccessControlContext());
    }

    /**
     * Returns result of calling ctx.isTrustedObject(obj) with thread-local state set to obj.
     */
    private static boolean isTrusted(Object obj, TrustVerifier.Context ctx)
            throws RemoteException {
        Object saved = state.get();
        try {
            state.set(obj);
            return ctx.isTrustedObject(obj);
        } finally {
            state.set(saved);
        }
    }

    /**
     * Takes a bootstrap proxy that doesn't have an iterator method and whose invocation handler
     * doesn't have an iterator method. If its class loader is a proper RMI child of its parent,
     * creates a new bootstrap proxy in the parent, and if it's trusted, makes remote call to get
     * verifier, and then conditionally remarshals the verifier. Remarshals if the verifier's class
     * loader is not the original bootstrap proxy's class loader, but the reserialized form of the
     * verifier contains a descriptor for a class with the same codebase as but different class
     * loader than the original bootstrap proxy.
     */
    private static TrustVerifier getAltVerifier(Object obj,
                                                TrustVerifier.Context ctx,
                                                MethodConstraints mc)
            throws RemoteException {
        if (obj == state.get()) {
            return null;
        }
        final Class base = obj.getClass();
        final String bcb = RMIClassLoader.getClassAnnotation(base);
        if (bcb == null || bcb.length() == 0) {
            return null;
        }
        final InvocationHandler ih = Proxy.getInvocationHandler(obj);
        obj = AccessController.doPrivileged(new PrivilegedAction() {
            public Object run() {
                ClassLoader bcl = base.getClassLoader();
                if (bcl == null) {
                    return null;
                }
                ClassLoader pcl = bcl.getParent();
                Thread t = Thread.currentThread();
                ClassLoader ccl = t.getContextClassLoader();
                boolean proper = false;
                try {
                    t.setContextClassLoader(pcl);
                    proper = (RMIClassLoader.getClassLoader(bcb) == bcl);
                } catch (MalformedURLException e) {
                } finally {
                    t.setContextClassLoader(ccl);
                }
                if (proper) {
                    try {
                        return Proxy.newProxyInstance(
                                pcl,
                                new Class[]{ProxyTrust.class,
                                        RemoteMethodControl.class},
                                ih);
                    } catch (IllegalArgumentException e) {
                    }
                }
                return null;
            }
        });
        if (obj == null) {
            return null;
        }
        logger.log(Level.FINER, "trying derivative bootstrap proxy {0}", obj);
        if (!isTrusted(obj, ctx)) {
            return null;
        }
        obj = ((RemoteMethodControl) obj).setConstraints(mc);
        TrustVerifier verifier = ((ProxyTrust) obj).getProxyVerifier();
        final Class vc = verifier.getClass();
        ClassLoader bcl = (ClassLoader)
                AccessController.doPrivileged(new PrivilegedAction() {
                    public Object run() {
                        ClassLoader bcl = base.getClassLoader();
                        if (bcl == vc.getClassLoader()) {
                            return null;
                        }
                        return bcl;
                    }
                });
        if (bcl != null) {
            try {
                ByteArrayOutputStream bout = new ByteArrayOutputStream();
                MOStream out = new MOStream(bout, bcb, bcl);
                out.writeObject(verifier);
                out.close();
                if (out.replace) {
                    logger.log(Level.FINER, "remarshalling verifier");
                    MarshalInputStream in =
                            new MarshalInputStream(
                                    new ByteArrayInputStream(bout.toByteArray()),
                                    bcl, false, null, Collections.EMPTY_SET);
                    in.useCodebaseAnnotations();
                    verifier = (TrustVerifier) in.readObject();
                    in.close();
                }
            } catch (IOException e) {
                throw new UnmarshalException("remarshalling verifier failed",
                        e);
            } catch (ClassNotFoundException e) {
                throw new UnmarshalException("remarshalling verifier failed",
                        e);
            }
        }
        logger.log(Level.FINE, "verifier is {0}", verifier);
        return verifier;
    }

    /**
     * Marshal output stream that looks for a class with a given codebase but in a different class
     * loader than the one we have.
     */
    private static class MOStream
            extends ObjectOutputStream implements ObjectStreamContext {
        /**
         * bootstrap proxy codebase
         */
        private final String bcb;
        /**
         * bootstrap proxy class loader
         */
        private final ClassLoader bcl;
        /**
         * true if we see a class with same codebase in different loader
         */
        boolean replace = false;

        MOStream(ByteArrayOutputStream out, String bcb, ClassLoader bcl)
                throws IOException {
            super(out);
            this.bcb = bcb;
            this.bcl = bcl;
        }

        public Collection getObjectStreamContext() {
            return Collections.EMPTY_SET;
        }

        @Override
        protected void annotateClass(Class c) throws IOException {
            writeAnnotation(c);
        }

        @Override
        protected void annotateProxyClass(Class c) throws IOException {
            writeAnnotation(c);
        }

        private void writeAnnotation(final Class c) throws IOException {
            String cb = RMIClassLoader.getClassAnnotation(c);
            writeObject(cb);
            if (bcb.equals(cb)) {
                AccessController.doPrivileged(new PrivilegedAction() {
                    public Object run() {
                        if (c.getClassLoader() != bcl) {
                            replace = true;
                        }
                        return null;
                    }
                });
            }
        }
    }

    /**
     * Returns getProxyTrustIterator method of object if it has a proper one.
     */
    private static Method getMethod(Object obj) {
        final Class base = obj.getClass();
        return (Method) AccessController.doPrivileged(new PrivilegedAction() {
            public Object run() {
                for (Class c = base; c != null; c = c.getSuperclass()) {
                    try {
                        Method m =
                                c.getDeclaredMethod("getProxyTrustIterator",
                                        new Class[0]);
                        if (usable(m, c, base)) {
                            m.setAccessible(true);
                            return m;
                        }
                        break;
                    } catch (NoSuchMethodException e) {
                    }
                }
                return null;
            }
        });
    }

    /**
     * Returns true if the method returns ProxyTrustIterator, has no declared exceptions, and is a
     * non-static member of the base class.
     */
    private static boolean usable(Method m, Class c, Class base) {
        int mods = m.getModifiers();
        return (m.getReturnType() == ProxyTrustIterator.class &&
                m.getExceptionTypes().length == 0 &&
                (mods & Modifier.STATIC) == 0 &&
                ((mods & (Modifier.PUBLIC | Modifier.PROTECTED)) != 0 ||
                        ((mods & Modifier.PRIVATE) != 0 ?
                                c == base : samePackage(c, base))));
    }

    /**
     * Returns true if the classes are in the same package, false otherwise.
     */
    private static boolean samePackage(Class c1, Class c2) {
        if (c1.getClassLoader() == c2.getClassLoader()) {
            String n1 = c1.getName();
            int i1 = n1.lastIndexOf('.');
            String n2 = c2.getName();
            int i2 = n2.lastIndexOf('.');
            return i1 == i2 && (i1 < 0 || n1.regionMatches(0, n2, 0, i1));
        }
        return false;
    }
}
