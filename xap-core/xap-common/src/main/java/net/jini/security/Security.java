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

package net.jini.security;

import com.gigaspaces.logger.LogUtils;
import com.sun.jini.collection.WeakIdentityMap;
import com.sun.jini.resource.Service;

import net.jini.security.policy.DynamicPolicy;
import net.jini.security.policy.SecurityContextSource;

import java.lang.ref.SoftReference;
import java.net.MalformedURLException;
import java.net.URL;
import java.rmi.RemoteException;
import java.security.AccessControlContext;
import java.security.AccessController;
import java.security.DomainCombiner;
import java.security.Permission;
import java.security.Policy;
import java.security.Principal;
import java.security.PrivilegedAction;
import java.security.PrivilegedActionException;
import java.security.PrivilegedExceptionAction;
import java.security.ProtectionDomain;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.StringTokenizer;
import java.util.WeakHashMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.security.auth.Subject;
import javax.security.auth.SubjectDomainCombiner;

/**
 * Provides methods for executing actions with privileges enabled, for snapshotting security
 * contexts, for verifying trust in proxies, for verifying codebase integrity, and for dynamically
 * granting permissions. This class cannot be instantiated.
 *
 * com.sun.jini.impl - This implementation uses the {@link Logger} named
 * <code>net.jini.security.integrity</code> to log information at the following levels: <table
 * summary="Describes what is logged by Security to the integrity logger at various logging levels"
 * border=1 cellpadding=5> <tr> <th>Level</th> <th>Description</th> </tr> <tr> <td>{@link
 * Level#FINE FINE}</td> <td><code>verifyCodebaseIntegrity</code> throws a
 * <code>SecurityException</code> because no integrity verifier verifies a URL</td> </tr> <tr>
 * <td>{@link Level#FINE FINE}</td> <td>integrity verifier returns <code>true</code></td> </tr> <tr>
 * <td>{@link Level#FINE FINE}</td> <td>creation of cached integrity verifiers</td> </tr> </table>
 * <p> This implementation uses the {@link Logger} named <code>net.jini.security.policy</code> to
 * log information at the following level: <table summary="Describes what is logged by Security to
 * the policy logger at various logging levels" border=1 cellpadding=5> <tr> <th>Level</th>
 * <th>Description</th> </tr> <tr> <td>{@link Level#FINER FINER}</td> <td>dynamic permission
 * grants</td> </tr> </table> <p> This implementation uses the {@link Logger} named
 * <code>net.jini.security.trust</code> to log information at the following levels: <table
 * summary="Describes what is logged by Security to the trust logger at various logging levels"
 * border=1 cellpadding=5> <tr> <th>Level</th> <th>Description</th> </tr> <tr> <td>{@link
 * Level#FINE FINE}</td> <td><code>verifyObjectTrust</code> throws a
 * <code>SecurityException</code> because no trust verifier trusts the specified object</td> </tr>
 * <tr> <td>{@link Level#FINE FINE}</td> <td><code>TrustVerifier.Context.isTrustedObject</code>
 * throws an exception</td> </tr> <tr> <td>{@link Level#FINE HANDLED}</td> <td>trust verifier
 * throws a <code>RemoteException</code> or a <code>SecurityException</code></td> </tr> <tr>
 * <td>{@link Level#FINE FINE}</td> <td>trust verifier returns <code>true</code></td> </tr> <tr>
 * <td>{@link Level#FINE FINE}</td> <td>creation of cached trust verifiers</td> </tr> <tr>
 * <td>{@link Level#FINE FINE}</td> <td><code>TrustVerifier.Context.isTrustedObject</code> returns
 * <code>false</code> because no trust verifier trusts the specified object</td> </tr> </table>
 *
 * @author Sun Microsystems, Inc.
 * @since 2.0
 */
public final class Security {

    private static final Logger trustLogger =
            LoggerFactory.getLogger("net.jini.security.trust");
    private static final Logger integrityLogger =
            LoggerFactory.getLogger("net.jini.security.integrity");
    private static final Logger policyLogger =
            LoggerFactory.getLogger("net.jini.security.policy");

    /**
     * Weak map from String to [URL[], SoftReference(key)]
     */
    private static Map pathToURLsCache = new WeakHashMap(5);
    /**
     * Weak map from ClassLoader to SoftReference(IntegrityVerifier[]).
     */
    private static final WeakIdentityMap integrityMap = new WeakIdentityMap();

    /**
     * SecurityManager instance used to obtain caller's Class.
     */
    private static final ClassContextAccess ctxAccess = (ClassContextAccess)
            AccessController.doPrivileged(new PrivilegedAction() {
                public Object run() {
                    return new ClassContextAccess();
                }
            });

    /**
     * Non-instantiable.
     */
    private Security() {
    }

    /**
     * Verifies that the specified object can be trusted to correctly implement its contract, using
     * verifiers from the specified class loader and using the specified collection of context
     * objects as necessary. If a <code>null</code> class loader is specified, the context class
     * loader of the current thread is used instead. Code that is itself downloaded and that carries
     * its own trust verifiers (to trust other downloaded code) should specify an explicit class
     * loader unless the calling code is known to be reachable from the context class loader. <p> A
     * {@link TrustVerifier.Context} is created, containing an ordered list of trust verifiers
     * (obtained as specified below) and the specified class loader and collection of context
     * objects. The {@link TrustVerifier.Context#isTrustedObject isTrustedObject} method of that
     * context is then called with the specified object. If that call returns <code>true</code>,
     * then this method returns normally. If that call throws a <code>RemoteException</code> or
     * <code>SecurityException</code> exception, that exception is thrown by this method. If that
     * call returns <code>false</code>, a <code>SecurityException</code> is thrown. <p> The
     * collection of context objects is provided as a means for the caller to communicate additional
     * information to the trust verifiers. The meaning of an element in this collection is
     * determined by its type. As a specific example, if any trust verifiers might communicate with
     * a remote server (in particular, when verifying a proxy for a remote server), the caller might
     * be responsible for specifying any necessary client constraints as a context object of type
     * {@link net.jini.core.constraint.MethodConstraints}. <p> When security is a concern, this
     * method should be called with a downloaded proxy before making any other use of the proxy, in
     * order to verify basic trust in the proxy to correctly implement its contract. This method can
     * also be used to verify trust in other types of objects, depending on what verifiers have been
     * configured. In general, verification of an object involves verification of all of its
     * constituent objects. However, for objects that are instances of {@link
     * net.jini.core.constraint.RemoteMethodControl}, the client constraints (that would be returned
     * by {@link net.jini.core.constraint.RemoteMethodControl#getConstraints
     * RemoteMethodControl.getConstraints}) are not verified; it is assumed that the caller will
     * either replace them or independently decide that it trusts them. Verification of other types
     * of objects may similarly exempt certain application-controlled state. <p> The list of trust
     * verifiers is obtained as follows. For each resource named <code>META-INF/services/net.jini.security.TrustVerifier</code>
     * that is visible to the specified class loader, the contents of the resource are parsed as
     * UTF-8 text to produce a list of class names. The resource must contain a list of fully
     * qualified class names, one per line. Space and tab characters surrounding each name, as well
     * as blank lines, are ignored.  The comment character is <tt>'#'</tt>; all characters on each
     * line starting with the first comment character are ignored. Each class name (that is not a
     * duplicate of any previous class name) is loaded through the specified class loader, and the
     * resulting class must be assignable to {@link TrustVerifier} and have a public no-argument
     * constructor. The constructor is invoked to create a trust verifier instance. An
     * implementation of this method is permitted to cache the verifier instances associated with a
     * class loader, rather than recreating them on every call.
     *
     * @param obj     the object in which to verify trust
     * @param loader  the class loader for finding trust verifiers, or <code>null</code> to use the
     *                context class loader
     * @param context a collection of context objects for use by trust verifiers
     * @throws SecurityException    if the object is not trusted, or if a <code>SecurityException</code>
     *                              is thrown by the trust verifier context
     * @throws RemoteException      if a communication-related exception occurs
     * @throws NullPointerException if the collection is <code>null</code>
     */
    public static void verifyObjectTrust(Object obj,
                                         ClassLoader loader,
                                         Collection context)
            throws RemoteException {
        if (context == null) {
            throw new NullPointerException("collection cannot be null");
        }
        if (new Context(loader, context).isTrustedObject(obj)) {
            return;
        }
        SecurityException e = new SecurityException(
                "object is not trusted: " + obj);
        if (trustLogger.isDebugEnabled()) {
            LogUtils.throwing(trustLogger, Security.class, "verifyObjectTrust", e,
                    "no verifier trusts {}", obj);
        }
        throw e;
    }

    /**
     * Verifies that the URLs in the specified codebase all provide content integrity, using
     * verifiers from the specified class loader. If a <code>null</code> class loader is specified,
     * the context class loader of the current thread is used instead. An ordered list of integrity
     * verifiers is obtained as specified below. For each URL (if any) in the specified codebase,
     * the {@link IntegrityVerifier#providesIntegrity providesIntegrity} method of each verifier is
     * called (in order) with the URL. If any verifier call returns <code>true</code>, the URL is
     * verified (and no further verifiers are called with that URL). If all of the verifier calls
     * return <code>false</code> for a URL, this method throws a <code>SecurityException</code>. If
     * all of the URLs are verified, this method returns normally. <p> The list of integrity
     * verifiers is obtained as follows. For each resource named <code>META-INF/services/net.jini.security.IntegrityVerifier</code>
     * that is visible to the specified class loader, the contents of the resource are parsed as
     * UTF-8 text to produce a list of class names. The resource must contain a list of fully
     * qualified class names, one per line. Space and tab characters surrounding each name, as well
     * as blank lines, are ignored.  The comment character is <tt>'#'</tt>; all characters on each
     * line starting with the first comment character are ignored. Each class name (that is not a
     * duplicate of any previous class name) is loaded through the specified class loader, and the
     * resulting class must be assignable to {@link IntegrityVerifier} and have a public no-argument
     * constructor. The constructor is invoked to create an integrity verifier instance. An
     * implementation of this method is permitted to cache the verifier instances associated with a
     * class loader, rather than recreating them on every call.
     *
     * @param codebase space-separated list of URLs, or <code>null</code>
     * @param loader   the class loader for finding integrity verifiers, or <code>null</code> to use
     *                 the context class loader
     * @throws MalformedURLException if the specified codebase contains an invalid URL
     * @throws SecurityException     if any URL in the specified codebase does not provide content
     *                               integrity
     */
    public static void verifyCodebaseIntegrity(String codebase,
                                               ClassLoader loader)
            throws MalformedURLException {
        if (codebase == null) {
            return;
        }
        if (loader == null) {
            loader = getContextClassLoader();
        }
        URL[] urls = pathToURLs(codebase);
        IntegrityVerifier[] verifiers = getIntegrityVerifiers(loader);
        outer:
        for (int i = urls.length; --i >= 0; ) {
            for (int j = 0; j < verifiers.length; j++) {
                if (verifiers[j].providesIntegrity(urls[i])) {
                    if (integrityLogger.isDebugEnabled()) {
                        integrityLogger.debug(
                                "{0} verifies {1}",
                                new Object[]{verifiers[j],
                                        urls[i]});
                    }
                    continue outer;
                }
            }
            SecurityException e =
                    new SecurityException("URL does not provide integrity: " +
                            urls[i]);
            if (integrityLogger.isDebugEnabled()) {
                LogUtils.throwing(integrityLogger,
                        Security.class, "verifyCodebaseIntegrity", e,
                        "no verifier verifies {}", urls[i]);
            }
            throw e;
        }
    }

    /**
     * Convert a string containing a space-separated list of URLs into a corresponding array of URL
     * objects, throwing a MalformedURLException if any of the URLs are invalid.
     */
    private static URL[] pathToURLs(String path) throws MalformedURLException {
        synchronized (pathToURLsCache) {
            Object[] v = (Object[]) pathToURLsCache.get(path);
            if (v != null) {
                return (URL[]) v[0];
            }
        }
        StringTokenizer st = new StringTokenizer(path);    // divide by spaces
        URL[] urls = new URL[st.countTokens()];
        for (int i = 0; st.hasMoreTokens(); i++) {
            urls[i] = new URL(st.nextToken());
        }
        synchronized (pathToURLsCache) {
            pathToURLsCache.put(path,
                    new Object[]{urls, new SoftReference(path)});
        }
        return urls;
    }

    /**
     * Return the integrity verifiers for the specified class loader.
     */
    private static IntegrityVerifier[] getIntegrityVerifiers(
            final ClassLoader cl) {
        SoftReference ref;
        synchronized (integrityMap) {
            ref = (SoftReference) integrityMap.get(cl);
        }
        IntegrityVerifier[] verifiers = null;
        if (ref != null) {
            verifiers = (IntegrityVerifier[]) ref.get();
        }
        if (verifiers == null) {
            final ArrayList list = new ArrayList(1);
            AccessController.doPrivileged(new PrivilegedAction() {
                public Object run() {
                    for (Iterator iter =
                         Service.providers(IntegrityVerifier.class, cl);
                         iter.hasNext(); ) {
                        list.add(iter.next());
                    }
                    return null;
                }
            });
            if (integrityLogger.isDebugEnabled()) {
                integrityLogger.debug(LogUtils.format(Security.class, "verifyCodebaseIntegrity",
                        "integrity verifiers {}", list));
            }
            verifiers = (IntegrityVerifier[]) list.toArray(
                    new IntegrityVerifier[list.size()]);
            synchronized (integrityMap) {
                integrityMap.put(cl, new SoftReference(verifiers));
            }
        }
        return verifiers;
    }

    /**
     * Returns a snapshot of the current security context, which can be used to restore the context
     * at a later time.  If either the installed security manager or policy provider implements the
     * {@link SecurityContextSource} interface, then this method delegates to the {@link
     * SecurityContextSource#getContext getContext} method of the implementing object, with
     * precedence given to the security manager.  If neither the security manager nor the policy
     * provider implement <code>SecurityContextSource</code>, then a new default {@link
     * SecurityContext} instance is returned whose methods have the following semantics: <ul>
     * <li>The <code>wrap</code> methods each return their respective <code>PrivilegedAction</code>
     * and <code>PrivilegedExceptionAction</code> arguments, unmodified <li>The
     * <code>getAccessControlContext</code> method returns the <code>AccessControlContext</code> in
     * effect when the security context was created </ul>
     *
     * @return snapshot of the current security context
     */
    public static SecurityContext getContext() {
        SecurityManager sm = System.getSecurityManager();
        if (sm instanceof SecurityContextSource) {
            return ((SecurityContextSource) sm).getContext();
        }
        Policy policy = getPolicy();
        if (policy instanceof SecurityContextSource) {
            return ((SecurityContextSource) policy).getContext();
        }

        final AccessControlContext acc = AccessController.getContext();
        return new SecurityContext() {
            public PrivilegedAction wrap(PrivilegedAction a) {
                if (a == null) {
                    throw new NullPointerException();
                }
                return a;
            }

            public PrivilegedExceptionAction wrap(PrivilegedExceptionAction a) {
                if (a == null) {
                    throw new NullPointerException();
                }
                return a;
            }

            public AccessControlContext getAccessControlContext() {
                return acc;
            }
        };
    }

    /**
     * Executes the specified action's <code>run</code> method with privileges enabled, preserving
     * the domain combiner (if any) of the calling context. If the action's <code>run</code> method
     * throws an unchecked exception, that exception is thrown by this method.  This method is
     * equivalent to the {@link AccessController#doPrivileged(PrivilegedAction)
     * AccessController.doPrivileged} method of the same signature, except that it maintains,
     * instead of clears, the domain combiner (if any) in place at the time of the call.  This
     * typically results in preservation of the current {@link Subject} (if the combiner is a {@link
     * SubjectDomainCombiner}), thus retaining permissions granted to principals of the
     * <code>Subject</code>, as well as the ability to use credentials of the <code>Subject</code>
     * for authentication.
     *
     * @param action the action to be executed
     * @return the object returned by the action's <code>run</code> method
     * @throws NullPointerException if the action is <code>null</code>
     */
    public static Object doPrivileged(final PrivilegedAction action) {
        final Class caller = ctxAccess.getCaller();
        final AccessControlContext acc = AccessController.getContext();
        return AccessController.doPrivileged(new PrivilegedAction() {
            public Object run() {
                return AccessController.doPrivileged(
                        action, createPrivilegedContext(caller, acc));
            }
        });
    }

    /**
     * Executes the specified action's <code>run</code> method with privileges enabled, preserving
     * the domain combiner (if any) of the calling context. If the action's <code>run</code> method
     * throws an unchecked exception, that exception is thrown by this method.  This method is
     * equivalent to the {@link AccessController#doPrivileged(PrivilegedExceptionAction)
     * AccessController.doPrivileged} method of the same signature, except that it maintains,
     * instead of clears, the domain combiner (if any) in place at the time of the call.  This
     * typically results in preservation of the current <code>Subject</code> (if the combiner is a
     * <code>SubjectDomainCombiner</code>), thus retaining permissions granted to principals of the
     * <code>Subject</code>, as well as the ability to use credentials of the <code>Subject</code>
     * for authentication.
     *
     * @param action the action to be executed
     * @return the object returned by the action's <code>run</code> method
     * @throws PrivilegedActionException if the action's <code>run</code> method throws a checked
     *                                   exception
     * @throws NullPointerException      if the action is <code>null</code>
     */
    public static Object doPrivileged(final PrivilegedExceptionAction action)
            throws PrivilegedActionException {
        final Class caller = ctxAccess.getCaller();
        final AccessControlContext acc = AccessController.getContext();
        return AccessController.doPrivileged(new PrivilegedExceptionAction() {
            public Object run() throws Exception {
                try {
                    return AccessController.doPrivileged(
                            action, createPrivilegedContext(caller, acc));
                } catch (PrivilegedActionException e) {
                    throw e.getException();
                }
            }
        });
    }

    /**
     * Creates privileged context that contains the protection domain of the given caller class (if
     * non-null) and uses the domain combiner of the specified context.  This method assumes it is
     * called from within a privileged block.
     */
    private static AccessControlContext createPrivilegedContext(
            Class caller,
            AccessControlContext acc) {
        DomainCombiner comb = acc.getDomainCombiner();
        ProtectionDomain pd = caller.getProtectionDomain();
        ProtectionDomain[] pds = (pd != null) ?
                new ProtectionDomain[]{pd} : null;
        if (comb != null) {
            pds = comb.combine(pds, null);
        }
        if (pds == null) {
            pds = new ProtectionDomain[0];
        }
        return new AccessControlContext(new AccessControlContext(pds), comb);
    }

    /**
     * Returns <code>true</code> if the installed security policy provider supports dynamic
     * permission grants--i.e., if it implements the {@link DynamicPolicy} interface and calling its
     * {@link DynamicPolicy#grantSupported grantSupported} method returns <code>true</code>. Returns
     * <code>false</code> otherwise.
     *
     * @return <code>true</code> if the installed security policy provider supports dynamic
     * permission grants
     * @see #grant(Class, Permission[])
     * @see #grant(Class, Principal[], Permission[])
     * @see #grant(Class, Class)
     */
    public static boolean grantSupported() {
        Policy policy = getPolicy();
        return (policy instanceof DynamicPolicy &&
                ((DynamicPolicy) policy).grantSupported());
    }

    /**
     * If the installed security policy provider implements the {@link DynamicPolicy} interface,
     * delegates to the security policy provider to grant the specified permissions to all
     * protection domains (including ones not yet created) that are associated with the class loader
     * of the given class and possess at least the principals of the current subject (if any).  If
     * the given class is <code>null</code>, then the grant applies across all protection domains
     * that possess at least the current subject's principals.  The current subject is determined by
     * calling {@link Subject#getSubject Subject.getSubject} on the context returned by {@link
     * AccessController#getContext AccessController.getContext}.  If the current subject is
     * <code>null</code> or has no principals, then principals are effectively ignored in
     * determining the protection domains to which the grant applies. <p> The given class, if
     * non-<code>null</code>, must belong to either the system domain or a protection domain whose
     * associated class loader is non-<code>null</code>.  If the class does not belong to such a
     * protection domain, then no permissions are granted and an <code>UnsupportedOperationException</code>
     * is thrown. <p> If a security manager is installed, its <code>checkPermission</code> method is
     * called with a {@link GrantPermission} containing the permissions to grant; if the permission
     * check fails, then no permissions are granted and the resulting <code>SecurityException</code>
     * is thrown. The permissions array passed in is neither modified nor retained; subsequent
     * changes to the array have no effect on the grant operation.
     *
     * @param cl          class to grant permissions to the class loader of, or <code>null</code> if
     *                    granting across all class loaders
     * @param permissions if non-<code>null</code>, permissions to grant
     * @throws UnsupportedOperationException if the installed security policy provider does not
     *                                       support dynamic permission grants, or if
     *                                       <code>cl</code> is non-<code>null</code> and belongs to
     *                                       a protection domain other than the system domain with
     *                                       an associated class loader of <code>null</code>
     * @throws SecurityException             if a security manager is installed and the calling
     *                                       context does not have <code>GrantPermission</code> for
     *                                       the given permissions
     * @throws NullPointerException          if any element of the permissions array is
     *                                       <code>null</code>
     * @see #grantSupported()
     * @see DynamicPolicy#grant(Class, Principal[], Permission[])
     */
    public static void grant(Class cl, Permission[] permissions) {
        grant(cl, getCurrentPrincipals(), permissions);
    }

    /**
     * If the installed security policy provider implements the {@link DynamicPolicy} interface,
     * delegates to the security policy provider to grant the specified permissions to all
     * protection domains (including ones not yet created) that are associated with the class loader
     * of the given class and possess at least the given set of principals.  If the given class is
     * <code>null</code>, then the grant applies across all protection domains that possess at least
     * the specified principals.  If the list of principals is <code>null</code> or empty, then
     * principals are effectively ignored in determining the protection domains to which the grant
     * applies. <p> The given class, if non-<code>null</code>, must belong to either the system
     * domain or a protection domain whose associated class loader is non-<code>null</code>.  If the
     * class does not belong to such a protection domain, then no permissions are granted and an
     * <code>UnsupportedOperationException</code> is thrown. <p> If a security manager is installed,
     * its <code>checkPermission</code> method is called with a <code>GrantPermission</code>
     * containing the permissions to grant; if the permission check fails, then no permissions are
     * granted and the resulting <code>SecurityException</code> is thrown. The principals and
     * permissions arrays passed in are neither modified nor retained; subsequent changes to the
     * arrays have no effect on the grant operation.
     *
     * @param cl          class to grant permissions to the class loader of, or <code>null</code> if
     *                    granting across all class loaders
     * @param principals  if non-<code>null</code>, minimum set of principals to which grants apply
     * @param permissions if non-<code>null</code>, permissions to grant
     * @throws UnsupportedOperationException if the installed security policy provider does not
     *                                       support dynamic permission grants, or if
     *                                       <code>cl</code> is non-<code>null</code> and belongs to
     *                                       a protection domain other than the system domain with
     *                                       an associated class loader of <code>null</code>
     * @throws SecurityException             if a security manager is installed and the calling
     *                                       context does not have <code>GrantPermission</code> for
     *                                       the given permissions
     * @throws NullPointerException          if any element of the principals or permissions arrays
     *                                       is <code>null</code>
     * @see #grantSupported()
     * @see DynamicPolicy#grant(Class, Principal[], Permission[])
     */
    public static void grant(Class cl,
                             Principal[] principals,
                             Permission[] permissions) {
        Policy policy = getPolicy();
        if (!(policy instanceof DynamicPolicy)) {
            throw new UnsupportedOperationException("grants not supported");
        }
        ((DynamicPolicy) policy).grant(cl, principals, permissions);
        if (policyLogger.isDebugEnabled()) {
            policyLogger.debug("granted {0} to {1}, {2}",
                    new Object[]{
                            (permissions != null) ? Arrays.asList(permissions) : null,
                            (cl != null) ? cl.getName() : null,
                            (principals != null) ? Arrays.asList(principals) : null});
        }
    }

    /**
     * If the installed security policy provider implements the {@link DynamicPolicy} interface,
     * takes the set of permissions dynamically granted to the class loader of
     * <code>fromClass</code> with the current subject's principals, determines which of those
     * permissions the calling context is authorized to grant, and dynamically grants that subset of
     * the permissions to the class loader of <code>toClass</code>, qualified with the current
     * subject's principals.  The current subject is determined by calling {@link Subject#getSubject
     * Subject.getSubject} on the context returned by {@link AccessController#getContext
     * AccessController.getContext}; the permissions dynamically granted to <code>fromClass</code>
     * are determined by calling the {@link DynamicPolicy#getGrants getGrants} method of the
     * currently installed policy, and the permission grant to <code>toClass</code> is performed by
     * invoking the {@link DynamicPolicy#grant grant} method of the current policy. <p> Both of the
     * given classes must be non-<code>null</code>, and must belong to either the system domain or a
     * protection domain whose associated class loader is non-<code>null</code>.  If either class
     * does not belong to such a protection domain, then no permissions are granted and an
     * <code>UnsupportedOperationException</code> is thrown.
     *
     * @param fromClass class indicating the source class loader of the dynamic grants to propagate
     * @param toClass   class indicating the target class loader of the dynamic grants to propagate
     * @throws NullPointerException          if <code>fromClass</code> or <code>toClass</code> is
     *                                       <code>null</code>
     * @throws UnsupportedOperationException if currently installed policy does not support dynamic
     *                                       permission grants, or if either specified class belongs
     *                                       to a protection domain with a <code>null</code> class
     *                                       loader, other than the system domain
     */
    public static void grant(Class fromClass, Class toClass) {
        if (fromClass == null || toClass == null) {
            throw new NullPointerException();
        }
        Policy policy = getPolicy();
        if (!(policy instanceof DynamicPolicy)) {
            throw new UnsupportedOperationException("grants not supported");
        }

        DynamicPolicy dpolicy = (DynamicPolicy) policy;
        Principal[] principals = getCurrentPrincipals();
        Permission[] permissions =
                grantablePermissions(dpolicy.getGrants(fromClass, principals));

        dpolicy.grant(toClass, principals, permissions);
        if (policyLogger.isDebugEnabled()) {
            policyLogger.debug("granted {0} from {1} to {2}, {3}",
                    new Object[]{
                            (permissions != null) ? Arrays.asList(permissions) : null,
                            fromClass.getName(),
                            toClass.getName(),
                            (principals != null) ? Arrays.asList(principals) : null});
        }
    }

    /**
     * Returns current thread's context class loader.
     */
    private static ClassLoader getContextClassLoader() {
        return (ClassLoader)
                AccessController.doPrivileged(new PrivilegedAction() {
                    public Object run() {
                        return Thread.currentThread().getContextClassLoader();
                    }
                });
    }

    /**
     * Returns currently installed security policy, if any.
     */
    private static Policy getPolicy() {
        return (Policy) AccessController.doPrivileged(new PrivilegedAction() {
            public Object run() {
                return Policy.getPolicy();
            }
        });
    }

    /**
     * Returns subset of given permissions that is grantable given the current calling context.
     */
    private static Permission[] grantablePermissions(Permission[] permissions) {
        SecurityManager sm = System.getSecurityManager();
        if (sm == null || permissions.length == 0) {
            return permissions;
        }

        try {
            sm.checkPermission(new GrantPermission(permissions));
            return permissions;
        } catch (SecurityException e) {
        }

        ArrayList list = new ArrayList(permissions.length);
        for (int i = 0; i < permissions.length; i++) {
            try {
                Permission p = permissions[i];
                sm.checkPermission(new GrantPermission(p));
                list.add(p);
            } catch (SecurityException e) {
            }
        }
        return (Permission[]) list.toArray(new Permission[list.size()]);
    }

    /**
     * Returns principals of current subject, or null if no current subject.
     */
    private static Principal[] getCurrentPrincipals() {
        final AccessControlContext acc = AccessController.getContext();
        Subject s = (Subject) AccessController.doPrivileged(
                new PrivilegedAction() {
                    public Object run() {
                        return Subject.getSubject(acc);
                    }
                });
        if (s != null) {
            Set ps = s.getPrincipals();
            return (Principal[]) ps.toArray(new Principal[ps.size()]);
        } else {
            return null;
        }
    }

    /**
     * TrustVerifier.Context implementation.
     */
    private static class Context implements TrustVerifier.Context {
        /**
         * Trust verifiers.
         */
        private final TrustVerifier[] verifiers;
        /**
         * The class loader.
         */
        private final ClassLoader cl;
        /**
         * Caller context.
         */
        private final Collection context;

        /**
         * Weak map from ClassLoader to SoftReference(TrustVerifier[]).
         */
        private static final WeakIdentityMap map = new WeakIdentityMap();

        /**
         * Creates an instance containing the trust verifiers found from the specified class loader,
         * using the verifier cache and updating the cache as necessary.
         */
        Context(ClassLoader cl, Collection context) {
            this.cl = cl;
            if (cl == null) {
                cl = getContextClassLoader();
            }
            SoftReference ref;
            synchronized (map) {
                ref = (SoftReference) map.get(cl);
            }
            TrustVerifier[] verifiers = null;
            if (ref != null) {
                verifiers = (TrustVerifier[]) ref.get();
            }
            if (verifiers == null) {
                final ArrayList list = new ArrayList(1);
                final ClassLoader scl = cl;
                AccessController.doPrivileged(new PrivilegedAction() {
                    public Object run() {
                        for (Iterator iter =
                             Service.providers(TrustVerifier.class, scl);
                             iter.hasNext(); ) {
                            list.add(iter.next());
                        }
                        return null;
                    }
                });
                if (trustLogger.isDebugEnabled()) {
                    trustLogger.debug(LogUtils.format(Security.class, "verifyObjectTrust",
                            "trust verifiers {}", list));
                }
                verifiers = (TrustVerifier[]) list.toArray(
                        new TrustVerifier[list.size()]);
                synchronized (map) {
                    map.put(cl, new SoftReference(verifiers));
                }
            }
            this.verifiers = verifiers;
            this.context = context;
        }

        public boolean isTrustedObject(Object obj)
                throws RemoteException {
            if (obj == null) {
                return true;
            }
            Exception ex = null;
            for (int i = 0; i < verifiers.length; i++) {
                try {
                    if (verifiers[i].isTrustedObject(obj, this)) {
                        if (trustLogger.isDebugEnabled()) {
                            trustLogger.debug(
                                    "{0} trusts {1}",
                                    new Object[]{verifiers[i], obj});
                        }
                        return true;
                    }
                } catch (Exception e) {
                    boolean rethrow = (e instanceof RuntimeException &&
                            !(e instanceof SecurityException));
                    if (trustLogger.isDebugEnabled()) {
                        LogUtils.throwing(trustLogger, this.getClass(), "isTrustedObject", e,
                                "{} checking {} throws", verifiers[i], obj);
                    }
                    if (rethrow) {
                        throw (RuntimeException) e;
                    }
                    ex = e;
                }
            }
            if (ex != null) {
                if (trustLogger.isDebugEnabled()) {
                    LogUtils.throwing(trustLogger, this.getClass(), "isTrustedObject", ex,
                            "checking {} throws", obj);
                }
                if (ex instanceof RemoteException) {
                    throw (RemoteException) ex;
                }
                throw (SecurityException) ex;
            }
            if (trustLogger.isDebugEnabled()) {
                trustLogger.debug("no verifier trusts {}", obj);
            }
            return false;
        }

        public ClassLoader getClassLoader() {
            return cl;
        }

        public Collection getCallerContext() {
            return context;
        }
    }

    /**
     * Dummy security manager providing access to getClassContext method.
     */
    private static class ClassContextAccess extends SecurityManager {
        /**
         * Returns caller's caller class.
         */
        Class getCaller() {
            return getClassContext()[2];
        }
    }
}
