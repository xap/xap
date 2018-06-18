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

package org.openspaces.jpa;

import org.apache.openjpa.conf.OpenJPAConfiguration;
import org.apache.openjpa.kernel.AbstractBrokerFactory;
import org.apache.openjpa.lib.conf.ConfigurationProvider;
import org.openspaces.jpa.openjpa.SpaceConfiguration;

/**
 * GigaSpaces OpenJPA BrokerFactory implementation.
 *
 * @author Idan Moyal
 * @since 8.0.1
 */
public class BrokerFactory extends AbstractBrokerFactory {
    //
    private static final long serialVersionUID = 1L;

    protected BrokerFactory(OpenJPAConfiguration config) {
        super(config);
    }

    @Override
    protected StoreManager newStoreManager() {
        return new org.openspaces.jpa.StoreManager();
    }

    /**
     * Factory method for constructing a {@link org.apache.openjpa.kernel.BrokerFactory}.
     */
    public static BrokerFactory newInstance(ConfigurationProvider cp) {
        StoreManager store = new org.openspaces.jpa.StoreManager();
        SpaceConfiguration conf = new SpaceConfiguration();
        cp.setInto(conf);
        conf.supportedOptions().removeAll(store.getUnsupportedOptions());

        return new BrokerFactory(conf);
    }


}
