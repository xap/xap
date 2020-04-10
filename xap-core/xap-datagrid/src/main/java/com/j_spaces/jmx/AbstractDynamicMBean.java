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

package com.j_spaces.jmx;

import com.gigaspaces.logger.Constants;
import com.j_spaces.kernel.ClassLoaderHelper;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.management.Attribute;
import javax.management.AttributeList;
import javax.management.AttributeNotFoundException;
import javax.management.DynamicMBean;
import javax.management.InvalidAttributeValueException;
import javax.management.MBeanAttributeInfo;
import javax.management.MBeanConstructorInfo;
import javax.management.MBeanException;
import javax.management.MBeanInfo;
import javax.management.MBeanNotificationInfo;
import javax.management.MBeanOperationInfo;
import javax.management.ReflectionException;
import javax.management.RuntimeOperationsException;


/*******************************************************************************
 * Copyright (c) 2010 GigaSpaces Technologies Ltd. All rights reserved
 *
 * The software source code is proprietary and confidential information of GigaSpaces. You may use
 * the software source code solely under the terms and limitations of The license agreement granted
 * to you by GigaSpaces.
 *******************************************************************************/
public abstract class AbstractDynamicMBean implements DynamicMBean {
    protected final String THIS_CLASS_NAME = this.getClass().getName();
    private String m_description;
    final private Hashtable<String, MBeanAttributeInfo> m_attributesMap;
    final private List<MBeanAttributeInfo> m_attributes;
    final private List<MBeanOperationInfo> m_operations;
    final private List<MBeanNotificationInfo> m_notifications;
    final private List<MBeanConstructorInfo> m_constructors;
    //logger
    final private static Logger _logger = LoggerFactory.getLogger(Constants.LOGGER_JMX);

    protected AbstractDynamicMBean() {
        m_attributesMap = new Hashtable<String, MBeanAttributeInfo>();
        m_attributes = new ArrayList<MBeanAttributeInfo>();
        m_operations = new ArrayList<MBeanOperationInfo>();
        m_notifications = new ArrayList<MBeanNotificationInfo>();
        m_constructors = new ArrayList<MBeanConstructorInfo>();
    }

    protected abstract void __setConfig(Object config);

    protected abstract Object __getConfig();

    protected abstract String getType();

    public void addMBeanAttributeInfo(MBeanAttributeInfo info) {
        m_attributes.add(info);
        m_attributesMap.put(info.getName(), info);
    }

    public void addMBeanOperationInfo(MBeanOperationInfo info) {
        m_operations.add(info);
    }

    public void addMBeanNotificationInfo(MBeanNotificationInfo info) {
        m_notifications.add(info);
    }

    public void addMBeanConstructorInfo(MBeanConstructorInfo info) {
        m_constructors.add(info);
    }

    public void setMBeanDescription(String descr) {
        m_description = descr;
    }

    //**********************************************************
    //*  The MBean implementor should override next methods in
    //*  order to provide the custom MBean metadata information.
    //**********************************************************

    protected MBeanAttributeInfo[] createMBeanAttributesInfo() {
        return m_attributes.toArray(new MBeanAttributeInfo[m_attributes.size()]);
    }

    protected MBeanOperationInfo[] createMBeanOperationsInfo() {
        return m_operations.toArray(new MBeanOperationInfo[m_operations.size()]);
    }

    protected MBeanNotificationInfo[] createMBeanNotificationsInfo() {
        return m_notifications.toArray(new MBeanNotificationInfo[m_notifications.size()]);
    }

    protected MBeanConstructorInfo[] createMBeanConstructorsInfo() {
        if (m_constructors.size() <= 0)
            return createMBeanConstructorsInfo1();
        else
            return m_constructors.toArray(new MBeanConstructorInfo[m_constructors.size()]);
    }

    protected String getMBeanDescription() {
        return m_description;
    }
    //***************************
    //* End for override methods
    //***************************

    private MBeanConstructorInfo[] createMBeanConstructorsInfo1() {
        Constructor[] constructors = this.getClass().getConstructors();
        MBeanConstructorInfo[] dConstructors = new MBeanConstructorInfo[constructors.length];
        for (int i = 0; i < constructors.length; i++) {
            //if (constructors[i].getParameterTypes().length > 0)
            dConstructors[i] = new MBeanConstructorInfo(
                    "Instantiate " + THIS_CLASS_NAME + " with the specified container.",
                    constructors[i]);
        }
        return dConstructors;
    }

    /**
     * Convert a specified character of string in upper case.
     */
    protected String toUpperCase(String str, int charIndex) {
        char[] name = str.toCharArray();
        name[0] = Character.toUpperCase(name[charIndex]);
        return new String(name);
    }

    protected String toLowCase(String str, int charIndex) {
        char[] name = str.toCharArray();
        name[0] = Character.toLowerCase(name[charIndex]);
        return new String(name);
    }


    //************************************************
    //* IMPLEMENTATION OF THE DynamicMBean INTERFACE
    //************************************************

    /**
     * Allows the value of the specified attribute of the Dynamic MBean to be obtained.
     */
    public Object getAttribute(String attributeName)
            throws AttributeNotFoundException, MBeanException, ReflectionException {
        if (attributeName == null || attributeName.trim().length() <= 0)
            throw new RuntimeOperationsException((new IllegalArgumentException
                    ("Attribute name can not be null or empty")),
                    "Can not invoke a getter of " + THIS_CLASS_NAME +
                            " with a null or empty attribute name string.");

        MBeanAttributeInfo attrInfo = m_attributesMap.get(attributeName);
        if (attrInfo == null)
            throw new AttributeNotFoundException("Can not find " +
                    attributeName + " attribute in " + THIS_CLASS_NAME);

        if (!attrInfo.isReadable())
            throw (new AttributeNotFoundException("Can not get attribute [" + attributeName + "] because it is not readable."));

        /** In first, try to obtain an attribute value by method invocation */
        try {
            String methodPrefix = "get";
            if (attrInfo.isIs())
                methodPrefix = "is";

            return getClass().getMethod(methodPrefix + toUpperCase(attributeName, 0)).invoke(this);
        } catch (NoSuchMethodException e) {
            //try to retrieve field with the same files name
            try {
                Object result = null;
                Object objConfig = __getConfig();

                /** Try to obtain attribute value through property */
                if (objConfig instanceof Properties) {
                    String value = ((Properties) objConfig).getProperty(attributeName);
                    if (value != null) {
                        result = ClassLoaderHelper.loadClass(attrInfo.getType()).getConstructor(
                                new Class[]{String.class}).newInstance(new Object[]{value});
                    }
                }
                /** Finally, try to obtain attribute value through object member/field */
                if (result == null)
                    result = objConfig.getClass().getField(toLowCase(attributeName, 0)).get(objConfig);

                return result;
            }
            /** If attribute has not been recognized throw an AttributeNotFoundException */ catch (NoSuchFieldException ex1) {
                if (_logger.isDebugEnabled()) {
                    _logger.debug(ex1.toString(), ex1);
                }

                throw new AttributeNotFoundException("Can not find " +
                        attributeName + " attribute in " + THIS_CLASS_NAME);
            } catch (Exception ex2) {
                if (_logger.isDebugEnabled()) {
                    _logger.debug(ex2.toString(), ex2);
                }
                throw new MBeanException(new Exception(e.getCause()));
            }
        } catch (InvocationTargetException ex) {
            Throwable targetException = ex.getTargetException();
            if (_logger.isDebugEnabled()) {
                _logger.debug(targetException.toString(), targetException);
            }
            throw new MBeanException((Exception) targetException);
        } catch (Exception e) {
            if (_logger.isDebugEnabled()) {
                _logger.debug(e.toString(), e);
            }

            throw new MBeanException(e);
        }
    }

    /**
     * Sets the value of the specified attribute of the Dynamic MBean.
     */
    public void setAttribute(Attribute attribute)
            throws AttributeNotFoundException, InvalidAttributeValueException,
            MBeanException, ReflectionException {
        if (attribute == null)
            throw new RuntimeOperationsException((new IllegalArgumentException
                    ("Attribute parameter can not be null")),
                    ("Can not invoke setAttribute method of " + THIS_CLASS_NAME
                            + " with null attribute parameter"));

        String attributeName = attribute.getName();
        Object attributeValue = attribute.getValue();

        if (attributeName == null || attributeName.trim().equals(""))
            throw new RuntimeOperationsException((new IllegalArgumentException
                    ("Name field of attribute parameter can not be null or empty")),
                    ("Can not invoke setAttribute method of " + THIS_CLASS_NAME
                            + " with null or empty attribute parameter name field"));

        MBeanAttributeInfo attrInfo = m_attributesMap.get(attributeName);
        if (attrInfo == null)
            throw new AttributeNotFoundException("Can not find " +
                    attributeName + " attribute in " + THIS_CLASS_NAME);
        else if (!attrInfo.isWritable())
            throw (new AttributeNotFoundException("Can not set attribute [" + attributeName + "] because it is not writable."));

        Class[] clazz = new Class[]{attributeValue.getClass()};
        Object objConfig = __getConfig();
        /** Try obtain a field and set attribute value */
        try {
            getClass().getMethod("set" + toUpperCase(attributeName, 0), clazz).invoke(this, new Object[]{attributeValue});
        }
        //catch (NoSuchMethodException ex)
        catch (Exception ex) {
            /** Try obtain a method and set attribute value. */
            try {
                objConfig.getClass().getField(toLowCase(attributeName, 0)).set(objConfig, attributeValue);
            } catch (NoSuchFieldException ex1) {
                /** Finally, try set attribute value as a property. */
                if (objConfig instanceof Properties)
                    ((Properties) objConfig).setProperty(attributeName, attributeValue.toString());
                else
                /** If attribute has not been recognized throw an AttributeNotFoundException */
                    throw (new AttributeNotFoundException("Cannot find " + attributeName + " attribute in " + THIS_CLASS_NAME));
            } catch (Exception ex2) {
                if (_logger.isWarnEnabled()) {
                    _logger.warn(ex2.toString(), ex2);
                }
                throw new MBeanException(new Exception(ex.getCause()));
            }
        }
    }

    /**
     * Enables the to get the values of several attributes of the Dynamic MBean.
     */
    public AttributeList getAttributes(String[] attributeNames) {
        if (attributeNames == null)
            throw new RuntimeOperationsException(new IllegalArgumentException(
                    "attributeNames[] can not be null"),
                    "Can not invoke a getter of " + THIS_CLASS_NAME);

        AttributeList resultList = new AttributeList();

        // if attributeNames is empty, return an empty result list
        if (attributeNames.length == 0)
            return resultList;

        // build the result attribute list
        for (int i = 0; i < attributeNames.length; i++) {
            try {
                Object value = getAttribute(attributeNames[i]);
                resultList.add(new Attribute(attributeNames[i], value));
            } catch (Exception e) {
                //we already log the exception in the method getAttribute()
            }
        }
        return resultList;
    }

    /**
     * Sets the values of several attributes of the Dynamic MBean, and returns the list of
     * attributes that have been set.
     */
    public AttributeList setAttributes(AttributeList attributes) {
        if (attributes == null)
            throw new RuntimeOperationsException(new IllegalArgumentException(
                    "AttributeList attributes can not be null"),
                    "Can not invoke a setter of " + THIS_CLASS_NAME);

        AttributeList resultList = new AttributeList();

        // if attributeNames is empty, nothing more to do
        if (attributes.isEmpty())
            return resultList;

        // for each attribute, try to set it and add to the result list if successful
        for (Iterator i = attributes.iterator(); i.hasNext(); ) {
            Attribute attr = (Attribute) i.next();
            try {
                setAttribute(attr);
                String name = attr.getName();
                Object value = getAttribute(name);
                resultList.add(new Attribute(name, value));
            } catch (Exception e) {
                if (_logger.isWarnEnabled()) {
                    _logger.warn(e.toString(), e);
                }
            }
        }
        return resultList;
    }

    /**
     * Allows an operation to be invoked on the Dynamic MBean.
     */
    public Object invoke(String operationName, Object[] params, String[] signature)
            throws MBeanException, ReflectionException {
        if (operationName == null)
            throw new RuntimeOperationsException(new IllegalArgumentException(
                    "Operation name can not be null"),
                    "Can not invoke a null operation in " + THIS_CLASS_NAME);

        // Check for a recognized operation name and call the corresponding operation
        try {
            Class[] clazz = null;
            if (signature != null) {
                clazz = new Class[signature.length];
                for (int i = 0; i < signature.length; i++)
                    clazz[i] = ClassLoaderHelper.loadClass(signature[i]);
            }

            return getClass().getMethod(operationName, clazz).invoke(this, params);

        }
        // unrecognized operation name
        catch (NoSuchMethodException ex) {
            if (_logger.isWarnEnabled()) {
                _logger.warn(ex.toString(), ex);
            }
            throw new ReflectionException(ex, "Can not find the operation " +
                    operationName + " in " + THIS_CLASS_NAME);
        } catch (Exception ex) {
            if (_logger.isWarnEnabled()) {
                _logger.warn(ex.toString(), ex);
            }
            throw new MBeanException(new Exception(ex.getCause()));
        }
    }

    /**
     * This method provides the exposed attributes and operations of the Dynamic MBean. It provides
     * this information using an MBeanInfo object.
     */
    public MBeanInfo getMBeanInfo() {
        MBeanInfo info = null;
        try {
            info = new MBeanInfo(THIS_CLASS_NAME, getMBeanDescription(),
                    createMBeanAttributesInfo(), createMBeanConstructorsInfo(),
                    createMBeanOperationsInfo(), createMBeanNotificationsInfo());
        } catch (Exception ex) {
            if (_logger.isWarnEnabled()) {
                _logger.warn(ex.toString(), ex);
            }
        }
        return info;

    }
}
