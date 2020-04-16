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

package com.j_spaces.jms;

import com.gigaspaces.logger.Constants;
import com.j_spaces.core.IJSpace;
import com.j_spaces.jms.utils.IMessageConverter;


import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.jms.JMSException;

/**
 * GigaSpaces implementation of the <code>javax.jms.XAQueueConnectionFactory</code> interface.
 */
public class GSXAQueueConnectionFactoryImpl
        extends GSXAConnectionFactoryImpl {
    private static final long serialVersionUID = 1L;

    //logger
    final private static Logger _logger = LoggerFactory.getLogger(Constants.LOGGER_JMS);

    /**
     * @param space
     * @param messageConverter
     * @throws JMSException
     */
    public GSXAQueueConnectionFactoryImpl(IJSpace space, IMessageConverter messageConverter)
            throws JMSException {
        super(space, messageConverter);
        if (space != null) {
            if (_logger.isDebugEnabled()) {
                _logger.debug("GSXAQueueConnectionFactoryImpl.GSXAQueueConnectionFactoryImpl():  spaceURL: " + space.getURL().getURL());
            }
        }
    }

    /**
     * @throws JMSException
     */
    public GSXAQueueConnectionFactoryImpl() throws JMSException {
        super(null, null);
    }
}//end of class
