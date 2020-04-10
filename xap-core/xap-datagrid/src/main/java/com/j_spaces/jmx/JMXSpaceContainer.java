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

import com.j_spaces.core.IJSpaceContainer;
import com.j_spaces.core.admin.ContainerConfig;
import com.j_spaces.core.admin.IJSpaceContainerAdmin;

import java.rmi.RemoteException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.management.Attribute;
import javax.management.AttributeList;
import javax.management.AttributeNotFoundException;
import javax.management.InvalidAttributeValueException;
import javax.management.MBeanException;
import javax.management.MBeanRegistration;
import javax.management.MBeanServer;
import javax.management.ObjectName;
import javax.management.ReflectionException;

/*******************************************************************************
 * Copyright (c) 2010 GigaSpaces Technologies Ltd. All rights reserved
 *
 * The software source code is proprietary and confidential information of GigaSpaces. You may use
 * the software source code solely under the terms and limitations of The license agreement granted
 * to you by GigaSpaces.
 *******************************************************************************/
@com.gigaspaces.api.InternalApi
public class JMXSpaceContainer
        extends XMLDescriptorsMBean
        implements MBeanRegistration {
    private IJSpaceContainer m_container;
    private ContainerConfig m_containerConfig;
    private MBeanServer m_mbeanServer;

    //logger
    final private static Logger _logger = LoggerFactory.getLogger(com.gigaspaces.logger.Constants.LOGGER_JMX);

    /**
     * This constructor receive instance of manageable container.
     */
    public JMXSpaceContainer(IJSpaceContainer container, String xmlDescriptorsFileURL)
            throws Exception {
        super(xmlDescriptorsFileURL);
        m_container = container;

        m_containerConfig = ((IJSpaceContainerAdmin) container).getConfig();
    }

    public JMXSpaceContainer() {
    }

    /**
     * Implements the abstract methods of AbstractDynamicMBean
     */
    @Override
    protected void __setConfig(Object config) {
        m_containerConfig = (ContainerConfig) config;
    }

    @Override
    protected Object __getConfig() {
        return m_containerConfig;
    }

    private void updateConfig() throws RemoteException {
        ((IJSpaceContainerAdmin) m_container).setConfig(m_containerConfig);
    }


    /*
     * Override method of AbstractDynamicMBean in order to update
     * container configuration after attributes altering.
     * With respect to Html Adaptor, this method will be invoke
     * after action on "Apply" button.
     * @see com.j_spaces.jmx.AbstractDynamicMBean#setAttributes(javax.management.AttributeList)
     */
    public AttributeList setAttributes(AttributeList attributes) {
        AttributeList result = super.setAttributes(attributes);
        try {
            updateConfig();
        } catch (Exception ex) {
            if (_logger.isWarnEnabled()) {
                _logger.warn(ex.toString(), ex);
            }
        }
        return result;
    }

    //************************************
    //* List of Container MBean Operations
    //************************************

    /**
     * Return the GigaSpaces Container.
     *
     * @return m_container
     */
    public IJSpaceContainer getContainer() {
        return m_container;
    }

    /**
     * Shuts down this container. This also involves unregistering all spaces from Lookup services
     * and closing connections to storage adapters.
     */
    public void shutdown() throws RemoteException {
        m_container.shutdown();
    }

//  **********************************************
    //* List of Container Attributes,
    //* that can not be processed through reflection
    //**********************************************

    /**
     * @return list of spaces contained in this container
     */
    public String[] getSpaceNames() throws RemoteException {
        return m_container.getSpaceNames();
    }

    /**
     * @return runtime config report
     */
    public String getRuntimeConfigReport() throws RemoteException {
        return ((IJSpaceContainerAdmin) m_container).getRuntimeConfigReport();
    }


    public Boolean isJndiRegistration() {
        return m_containerConfig.isJndiEnabled();
    }

    public void setJndiRegistration(Boolean b) {
        m_containerConfig.setJndiEnabled(b);
    }

    public Boolean isJiniRegistration() {
        return m_containerConfig.isJiniLusEnabled();
    }

    public void setJiniRegistration(Boolean b) {
        m_containerConfig.setJiniLusEnabled(b);
    }

    /**
     * @return the container schema used to create this container
     */
    public String getSchemaName() {
        return m_containerConfig.getSchemaName();
    }

    @Override
    public void setAttribute(Attribute attribute)
            throws AttributeNotFoundException, InvalidAttributeValueException,
            MBeanException, ReflectionException {
        super.setAttribute(attribute);
        try {
            updateConfig();
        } catch (RemoteException e) {
            throw new MBeanException(e);
        }
    }


    /********************************************
     * Implements of MBeanRegistration interface
     ********************************************/
    public ObjectName preRegister(MBeanServer mbs, ObjectName objName)
            throws Exception {
        m_mbeanServer = mbs;
        return objName;
    }

    public void postRegister(Boolean arg0) {
    }

    public void preDeregister() throws Exception {
    }

    public void postDeregister() {
    }

}
