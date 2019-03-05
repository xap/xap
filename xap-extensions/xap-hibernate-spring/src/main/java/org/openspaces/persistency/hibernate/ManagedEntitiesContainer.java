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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.hibernate.SessionFactory;
import org.hibernate.metadata.ClassMetadata;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * An managed entities container which is used by {@link AbstractHibernateSpaceDataSource} and
 * {@link AbstractHibernateSpaceSynchronizationEndpoint} based implementations.
 *
 * @author eitany
 * @since 9.5
 */
public class ManagedEntitiesContainer {

    protected static final Log logger = LogFactory.getLog(ManagedEntitiesContainer.class);

    private final Set<String> managedEntries;

    public ManagedEntitiesContainer(SessionFactory sessionFactory, Set<String> managedEntries) {
        this.managedEntries = createManagedEntries(managedEntries, sessionFactory);
    }

    private static Set<String> createManagedEntries(Set<String> managedEntries, SessionFactory sessionFactory) {
        if (managedEntries == null) {
            managedEntries = new HashSet<String>();
            // try and derive the managedEntries
            Map<String, ClassMetadata> allClassMetaData = sessionFactory.getAllClassMetadata();
            for (String entityname : allClassMetaData.keySet()) {
                managedEntries.add(entityname);
            }
        }
        if (logger.isDebugEnabled()) {
            logger.debug("Using Hibernate managedEntries [" + managedEntries + "]");
        }
        return managedEntries;
    }

    public boolean isManagedEntry(String entityName) {
        return managedEntries.contains(entityName);
    }

    public Iterable<String> getManagedEntries() {
        return managedEntries;
    }

}
