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

package org.openspaces.persistency.hibernate;

import org.hibernate.SessionFactory;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

/**
 * A configurer class which is used to configure a {@link StatelessHibernateSpaceSynchronizationEndpoint}.
 *
 * @author eitany
 * @since 9.5
 */
public class StatelessHibernateSpaceSynchronizationEndpointConfigurer {

    protected SessionFactory sessionFactory;

    protected Set<String> managedEntries;

    /**
     * Injects the Hibernate SessionFactory to be used with this synchronization endpoint
     * interceptor.
     */
    public StatelessHibernateSpaceSynchronizationEndpointConfigurer sessionFactory(SessionFactory sessionFactory) {
        this.sessionFactory = sessionFactory;
        return this;
    }

    /**
     * Sets all the entries this Hibernate synchronization endpoint interceptor will work with. By
     * default, will use Hibernate meta data API in order to get the list of all the given entities
     * it handles.
     *
     * <p>This list is used to filter out entities when performing all synchronization endpoint
     * interceptor operations.
     *
     * <p>Usually, there is no need to explicitly set this.
     */
    public StatelessHibernateSpaceSynchronizationEndpointConfigurer managedEntries(String... entries) {
        this.managedEntries = new HashSet<String>();
        this.managedEntries.addAll(Arrays.asList(entries));
        return this;
    }

    /**
     * Creates a {@link DefaultHibernateSpaceSynchronizationEndpoint} with the setup configuration.
     */
    public StatelessHibernateSpaceSynchronizationEndpoint create() {
        return new StatelessHibernateSpaceSynchronizationEndpoint(sessionFactory, managedEntries);
    }
}
