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
 * @(#)LRMIUtilities.java 1.0   01/02/2004  12:21:41
 */

package com.gigaspaces.lrmi;

import com.gigaspaces.annotation.lrmi.AsyncRemoteCall;
import com.gigaspaces.annotation.lrmi.CallBackRemoteCall;
import com.gigaspaces.annotation.lrmi.CustomTracking;
import com.gigaspaces.annotation.lrmi.LivenessPriority;
import com.gigaspaces.annotation.lrmi.MonitoringPriority;
import com.gigaspaces.annotation.lrmi.OneWayRemoteCall;
import com.gigaspaces.internal.io.GSByteArrayInputStream;
import com.gigaspaces.internal.io.GSByteArrayOutputStream;
import com.gigaspaces.internal.io.MarshalInputStream;
import com.gigaspaces.internal.io.MarshalOutputStream;
import com.gigaspaces.internal.reflection.IMethod;
import com.gigaspaces.internal.reflection.ReflectionUtil;
import com.gigaspaces.internal.version.PlatformLogicalVersion;
import com.gigaspaces.logger.Constants;
import com.gigaspaces.management.transport.ConnectionEndpointDetails;
import com.j_spaces.kernel.ClassLoaderHelper;
import com.j_spaces.kernel.SystemProperties;

import net.jini.export.UseStubCache;
import net.jini.security.Security;

import java.io.IOException;
import java.net.SocketException;
import java.nio.channels.SocketChannel;
import java.rmi.Remote;
import java.security.PrivilegedAction;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;


/**
 * This utility class provides a common Transport Protocol utilities methods.
 *
 * @author Igor Goldenberg
 * @see com.gigaspaces.lrmi.RemoteMethodCache
 * @since 4.0
 **/
@com.gigaspaces.api.InternalApi
public class LRMIUtilities {
    //logger
    final private static Logger _logger = Logger.getLogger(Constants.LOGGER_LRMI);

    final private static Comparator<IMethod> _methodComparator = new MethodComparator();

    /**
     * method comparator to sort the interface methods of specific object
     */
    final private static class MethodComparator
            implements Comparator<IMethod> {
        public int compare(IMethod o1, IMethod o2) {
            String method1 = o1.toString();
            String method2 = o2.toString();

            return method1.compareTo(method2);
        }
    }

    /**
     * Returns an array of all interfaces an lrmi based proxy to the specified class should
     * implement
     *
     * @param claz proxy class
     * @return an array of all interfaces an lrmi based proxy to the specified class should
     * implement
     */
    public static Class<?>[] getProxyInterfaces(Class<?> claz) {
        List<Class<?>> declaredInterfaces = getDeclaredRemoteInterfaces(claz);
        declaredInterfaces.add(ILRMIProxy.class);
        return declaredInterfaces.toArray(new Class[declaredInterfaces.size()]);
    }

    /**
     * Returns a list of all <code>java.rmi.Remote</code> interfaces implemented supplied class.
     *
     * @param claz The class to get all <code>java.rmi.Remote</code> interfaces.
     **/
    public static List<Class<?>> getDeclaredRemoteInterfaces(Class<?> claz) {
        return getDeclaredInterfaces(claz, Remote.class);
    }

    /**
     * Returns an array of all declared interfaces of desired class (super-classes comes an
     * account). If <code>assignableClasses</code> is <code>null</code> all implemented interface
     * will be added to array, otherwise only <b>assignable</b> classes.
     *
     * @param claz The class to get all interfaces.
     **/
    @SuppressWarnings("unchecked")
    public static List<Class<?>> getDeclaredInterfaces(Class claz, Class... assignableClasses) {
        ArrayList<Class<?>> interfList = new ArrayList<Class<?>>();

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

        return interfList;
    }


    /**
     * Returns a ~cache of sorted java.rmi.Remote methods of desired exported Remote object.
     **/
    public static RemoteMethodCache createRemoteMethodCache(List<Class<?>> remoteInterfaces, Map<String, Integer> methodMapping, Map<String, LRMIMethodMetadata> overrideMethodsMetadata) {
        /* get sorted remote interface methods of (Dynamic Proxy - shadow client side Remote Interface object)*/
        List<IMethod> sortedMethodList = getSortedMethodList(remoteInterfaces);

        LRMIMethod[] lrmiMethodList = wrapAsRemoteLRMIMethods(methodMapping, overrideMethodsMetadata, sortedMethodList);

        /* return the sorted methods cache of remote object */
        return new RemoteMethodCache(lrmiMethodList);
    }

    private static LRMIMethod[] wrapAsRemoteLRMIMethods(Map<String, Integer> methodMapping,
                                                        Map<String, LRMIMethodMetadata> overrideMethodsMetadata, List<IMethod> sortedMethodList) {
        /* initialize the array structure of sorted <code>LRMIMethod</code>. */
        int methodListSize = sortedMethodList.size();
        LRMIMethod[] lrmiMethodList = new LRMIMethod[methodListSize];

        for (int i = 0; i < methodListSize; i++) {
            IMethod interfMethod = sortedMethodList.get(i);
            
            /* checks whether this method of remote interface does have one-way annotation definition */
            boolean isOneWay = interfMethod.isAnnotationPresent(OneWayRemoteCall.class) ? true : OneWayMethodRepository.isOneWay(interfMethod);
            boolean isAsync = interfMethod.isAnnotationPresent(AsyncRemoteCall.class);
            final boolean isCallBack = interfMethod.isAnnotationPresent(CallBackRemoteCall.class);
            final boolean useStubCache = interfMethod.isAnnotationPresent(UseStubCache.class);
            final boolean livenessPriority = interfMethod.isAnnotationPresent(LivenessPriority.class);
            final boolean monitoringPriority = interfMethod.isAnnotationPresent(MonitoringPriority.class);
            final boolean isCustomTracking = interfMethod.isAnnotationPresent(CustomTracking.class);


            String methodNameAndDescriptor = getMethodNameAndDescriptor(interfMethod);
            if (overrideMethodsMetadata != null && overrideMethodsMetadata.containsKey(methodNameAndDescriptor)) {
                LRMIMethodMetadata lrmiMethodMetadata = overrideMethodsMetadata.get(methodNameAndDescriptor);
                if (lrmiMethodMetadata.isOneWayPresence()) {
                    isOneWay = lrmiMethodMetadata.isOneWay();
                    _logger.fine("LRMI override metadata one-way [" + isOneWay + "] : " + interfMethod);
                }
                if (lrmiMethodMetadata.isAsyncPresence()) {
                    isAsync = lrmiMethodMetadata.isAsync();
                }
            }

            if (isOneWay && _logger.isLoggable(Level.FINE)) {
                _logger.fine("LRMI @OneWayRemoteCallAnnotation - registered one-way method : " + interfMethod);
            }

            Integer methodOrder = methodMapping.get(methodNameAndDescriptor);
            lrmiMethodList[i] = methodOrder == null ? LRMIMethod.wrapAsUnsupported(interfMethod) :
                    new LRMIMethod(interfMethod, isOneWay, isCallBack, isAsync, useStubCache, livenessPriority, monitoringPriority, isCustomTracking, methodOrder);
        }
        return lrmiMethodList;
    }


    /**
     * returns the sorted RemoteInterface method list of desired class
     */
    static List<IMethod> getSortedMethodList(List<Class<?>> remoteInterfaces) {
        ArrayList<IMethod> methodList = new ArrayList<IMethod>();
        for (Class<?> cl : remoteInterfaces) {
          /* build the common method list before sorting */
            methodList.addAll(Arrays.asList(ReflectionUtil.createMethods(cl.getMethods())));
        }
       
       /* minimize the storage of an <tt>ArrayList</tt> instance. */
        methodList.trimToSize();
       
       /* make accessible, might be thrown security exception */
        setAccessible(methodList.toArray(new IMethod[methodList.size()]));

       /* sort all interfaces methods */
        Collections.sort(methodList, _methodComparator);

        return methodList;
    }

    /**
     * returns the sorted RemoteInterface method list of desired class
     */
    static LRMIMethod[] getSortedLRMIMethodList(Class<?> invokeObjClass) {
        List<Class<?>> interf = getDeclaredRemoteInterfaces(invokeObjClass);
        if (interf.isEmpty())
            throw new IllegalArgumentException("This class : " + invokeObjClass
                    + " must implement at least one java.rmi.Remote interface.");

        List<IMethod> sortedMethodList = getSortedMethodList(interf);
        List<LRMIMethod> sortedLrmiMethodList = new ArrayList<LRMIMethod>();
        for (int i = 0; i < sortedMethodList.size(); ++i) {
            IMethod interfMethod = sortedMethodList.get(i);
            /* checks whether this method of remote interface does have one-way annotation definition */
            boolean isOneWay = interfMethod.isAnnotationPresent(OneWayRemoteCall.class) ? true : OneWayMethodRepository.isOneWay(interfMethod);
            final boolean isCallBack = interfMethod.isAnnotationPresent(CallBackRemoteCall.class);
            final boolean isAsync = interfMethod.isAnnotationPresent(AsyncRemoteCall.class);
            final boolean useStubCache = interfMethod.isAnnotationPresent(UseStubCache.class);
            final boolean livenessPriority = interfMethod.isAnnotationPresent(LivenessPriority.class);
            final boolean monitoringPriority = interfMethod.isAnnotationPresent(MonitoringPriority.class);
            final boolean isCustomTracking = interfMethod.isAnnotationPresent(CustomTracking.class);

            LRMIMethod lrmiMethod = new LRMIMethod(interfMethod, isOneWay, isCallBack, isAsync, useStubCache, livenessPriority, monitoringPriority, isCustomTracking, i);
            sortedLrmiMethodList.add(lrmiMethod);
        }
        return sortedLrmiMethodList.toArray(new LRMIMethod[sortedLrmiMethodList.size()]);
    }

    private static void setAccessible(final IMethod[] methods) {
        Security.doPrivileged(new PrivilegedAction() {
            public Object run() {
                for (IMethod method : methods) {
                    method.setAccessible(true);
                }
                return null;
            }
        });
    }
    
    
    /*
     * The following static methods are related to the creation of the
     * "method table" for a remote class, which maps method hashes to
     * the appropriate Method objects for the class's remote methods.
     */

    /**
     * Returns a string consisting of the given method's name followed by its "method descriptor",
     * as appropriate for use in the computation of the "method hash".
     *
     * See section 4.3.3 of The Java(TM) Virtual Machine Specification for the definition of a
     * "method descriptor".
     */
    public static String getMethodNameAndDescriptor(IMethod m) {
        StringBuilder desc = new StringBuilder(m.getName());
        desc.append('(');
        Class[] paramTypes = m.getParameterTypes();
        for (Class paramType : paramTypes) {
            desc.append(getTypeDescriptor(paramType));
        }

        desc.append(')');
        Class returnType = m.getReturnType();
        if (returnType == void.class) {    // optimization: handle void here
            desc.append('V');
        } else {
            desc.append(getTypeDescriptor(returnType));
        }

        return desc.toString();
    }

    /**
     * Gets a readable format string that describe the specified method
     *
     * @param m displayed method
     * @return readable format string that describe the specified method
     * @since 7.1
     */
    public static String getMethodDisplayString(IMethod m) {
        if (m == null)
            return null;
        return m.getDeclaringClass().getName() + "." + m.getName();
    }


    /**
     * Returns the descriptor of a particular type, as appropriate for either a parameter type or
     * return type in a method descriptor.
     **/
    private static String getTypeDescriptor(Class type) {
        if (type.isPrimitive()) {
            if (type == int.class) {
                return "I";
            } else if (type == boolean.class) {
                return "Z";
            } else if (type == byte.class) {
                return "B";
            } else if (type == char.class) {
                return "C";
            } else if (type == short.class) {
                return "S";
            } else if (type == long.class) {
                return "J";
            } else if (type == float.class) {
                return "F";
            } else if (type == double.class) {
                return "D";
            } else if (type == void.class) {
                return "V";
            } else {
                throw new Error("unrecognized primitive type: " + type);
            }
        } else if (type.isArray()) {
            /*
   		  * According to JLS 20.3.2, the getName() method on Class does
   		  * return the virtual machine type descriptor format for array
   		  * classes (only); using that should be quicker than the otherwise
   		  * obvious code:
   		  *
   		  * return "[" + getTypeDescriptor(type.getComponentType());
   		  */
            return type.getName().replace('.', '/');
        } else {
            return "L" + type.getName().replace('.', '/') + ";";
        }
    }


    /**
     * Returns the cross instance/platform mapping table of desired class with method
     * descriptors(method hash). Key: String method descriptor (method hash) Value: Method instance
     * of provided class. <br> The method descriptor generates
     *
     * @param clazz the class to get mapping descriptors from.
     * @return the mapping descriptor table.
     **/
    static public Map<String, IMethod> getMappingMethodDescriptor(Class clazz) {
        List<Class<?>> interfClazzes = LRMIUtilities.getDeclaredInterfaces(clazz);
        Map<String, IMethod> implObjMap = new HashMap<String, IMethod>();

        for (Class<?> interfClass : interfClazzes) {
            final IMethod[] implObjMethods = ReflectionUtil.createMethods(interfClass.getMethods());
            setAccessible(implObjMethods);

            for (final IMethod m : implObjMethods) {
                String methodDesc = LRMIUtilities.getMethodNameAndDescriptor(m);
                implObjMap.put(methodDesc, m);
            }// for
        }// for

        return implObjMap;
    }

    /**
     * Convert the desired object into the new object instance assignable from provided ClassLoader.
     * NOTE: This method is very expensive and provided object to convert must be {@link
     * java.io.Serializable}.
     *
     * @param obj         the object to convert. Can be array as well.
     * @param classLoader the desired ClassLoader to convert the passed object.
     * @return the new instance of object derived from passed ClassLoader.
     * @throws IOException failed to convert
     **/
    static Object convertToAssignableClassLoader(Object obj, ClassLoader classLoader)
            throws IOException, ClassNotFoundException {

        try (GSByteArrayOutputStream oos = new GSByteArrayOutputStream();
             MarshalOutputStream mos = new MarshalOutputStream(oos)) {
            mos.writeObject(obj);
            mos.flush();

            try (GSByteArrayInputStream bais = new GSByteArrayInputStream(oos.toByteArray())) {
                ClassLoader origCL = Thread.currentThread().getContextClassLoader();
                try {
                    ClassLoaderHelper.setContextClassLoader(classLoader, true);
                    try (MarshalInputStream mis = new MarshalInputStream(bais)) {
                        return mis.readObject();
                    }
                } finally {
                    ClassLoaderHelper.setContextClassLoader(origCL, true);
                }
            }
        }
    }

    /**
     * @return true if the specified object is a proxy to a remote stub
     * @since 7.1
     */
    public static boolean isRemoteProxy(Object obj) {
        if (obj instanceof ILRMIProxy) {
            return ((ILRMIProxy) obj).isRemote();
        }
        return false;
    }

    public static PlatformLogicalVersion getServicePlatformLogicalVersion(Object obj) {
        if (obj instanceof ILRMIProxy) {
            return ((ILRMIProxy) obj).getServicePlatformLogicalVersion();
        }
        return PlatformLogicalVersion.getLogicalVersion();
    }

    public static ConnectionEndpointDetails getConnectionEndpointDetails(Object obj) {
        if (obj instanceof ILRMIProxy) {
            ILRMIProxy proxy = (ILRMIProxy) obj;
            return new ConnectionEndpointDetails(proxy.getRemoteHostName(),
                    proxy.getRemoteHostAddress(),
                    proxy.getRemoteProcessId(),
                    proxy.getServicePlatformLogicalVersion());
        }
        return null;
    }

    final static public int SEND_BUFFER_SIZE = Integer.getInteger(SystemProperties.LRMI_TCP_SEND_BUFFER, 0);
    final static public int RECEIVE_BUFFER_SIZE = Integer.getInteger(SystemProperties.LRMI_TCP_RECEIVE_BUFFER, 0);
    final static public boolean KEEP_ALIVE_MODE = Boolean.valueOf(System.getProperty(SystemProperties.LRMI_TCP_KEEP_ALIVE, String.valueOf(SystemProperties.LRMI_TCP_KEEP_ALIVE_DEFAULT)));
    final static public boolean TCP_NO_DELAY_MODE = Boolean.valueOf(System.getProperty(SystemProperties.LRMI_TCP_NO_DELAY, String.valueOf(SystemProperties.LRMI_TCP_NO_DELAY_DEFAULT)));
    final static public Integer TRAFFIC_CLASS = Integer.getInteger(SystemProperties.LRMI_TCP_TRAFFIC_CLASS);

    private static final long KILO = 1024;
    private static final long MEGA = KILO * KILO;
    private static final long GIGA = MEGA * KILO;
    private static final long TERA = GIGA * KILO;

    private static boolean _hadSetSendBufferSizeError = false;
    private static boolean _hadSetReceiveBufferSizeError = false;
    private static boolean _hadSetKeepAliveError = false;
    private static boolean _hadSetTcpNoDelayError = false;
    private static boolean _hadSetTrafficClassError = false;

    public static void initNewSocketProperties(SocketChannel sockChannel)
            throws SocketException {
        // Set the socket
        if (SEND_BUFFER_SIZE > 0) {
            try {
                sockChannel.socket().setSendBufferSize(SEND_BUFFER_SIZE);
            } catch (Exception e) {
                if (!_hadSetSendBufferSizeError) {
                    _hadSetSendBufferSizeError = true;
                    _logger.log(Level.WARNING, "Failed setting send buffer size [" + SEND_BUFFER_SIZE + "]", e);
                }
            }
        }
        if (RECEIVE_BUFFER_SIZE > 0) {
            try {
                sockChannel.socket().setReceiveBufferSize(RECEIVE_BUFFER_SIZE);
            } catch (Exception e) {
                if (!_hadSetReceiveBufferSizeError) {
                    _hadSetReceiveBufferSizeError = true;
                    _logger.log(Level.WARNING, "Failed setting receive buffer size [" + RECEIVE_BUFFER_SIZE + "]", e);
                }
            }

        }
        try {
            sockChannel.socket().setKeepAlive(KEEP_ALIVE_MODE);
        } catch (Exception e) {
            if (!_hadSetKeepAliveError) {
                _hadSetKeepAliveError = true;
                _logger.log(Level.WARNING, "Failed setting keep alive mode [" + KEEP_ALIVE_MODE + "]", e);
            }
        }
        try {
            sockChannel.socket().setTcpNoDelay(TCP_NO_DELAY_MODE);
        } catch (Exception e) {
            if (!_hadSetTcpNoDelayError) {
                _hadSetTcpNoDelayError = true;
                _logger.log(Level.WARNING, "Failed setting tcp no delay mode [" + TCP_NO_DELAY_MODE + "]", e);
            }
        }
        if (TRAFFIC_CLASS != null)
            try {
                sockChannel.socket().setTrafficClass(TRAFFIC_CLASS);
            } catch (Exception e) {
                if (!_hadSetTrafficClassError) {
                    _hadSetTrafficClassError = true;
                    _logger.log(Level.WARNING, "Failed setting traffic class [" + TRAFFIC_CLASS + "]", e);
                }
            }
    }

    private static final DecimalFormat DECIMAL_FORMAT = new DecimalFormat("#.##");

    public static String getTrafficString(long bytes) {
        if (bytes < KILO)
            return bytes + " bytes";
        if (bytes < MEGA)
            return DECIMAL_FORMAT.format((double) bytes / KILO) + " kilobytes";
        if (bytes < GIGA)
            return DECIMAL_FORMAT.format((double) bytes / MEGA) + " megabytes";
        if (bytes < TERA)
            return DECIMAL_FORMAT.format((double) bytes / GIGA) + " gigabytes";

        return DECIMAL_FORMAT.format((double) bytes / TERA) + " terabytes";
    }

}
