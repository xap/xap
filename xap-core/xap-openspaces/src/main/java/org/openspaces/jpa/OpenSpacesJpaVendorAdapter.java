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

import com.j_spaces.core.IJSpace;

import org.springframework.orm.jpa.JpaDialect;
import org.springframework.orm.jpa.JpaVendorAdapter;

import java.util.Map;

import javax.persistence.EntityManager;
import javax.persistence.EntityManagerFactory;
import javax.persistence.spi.PersistenceProvider;
import javax.persistence.spi.PersistenceUnitInfo;

/**
 * A spring OpenSpaces JPA vendor adapter.
 *
 * @author idan
 * @since 8.0
 */
public class OpenSpacesJpaVendorAdapter implements JpaVendorAdapter {

    private final OpenSpacesPersistenceProvider _persistenceProvider = new OpenSpacesPersistenceProvider();

    public PersistenceProvider getPersistenceProvider() {
        return _persistenceProvider;
    }

    public String getPersistenceProviderRootPackage() {
        return "org.openspaces.jpa";
    }

    public void setSpace(IJSpace space) {
        _persistenceProvider.setSpace(space);
    }

    public Class<? extends EntityManagerFactory> getEntityManagerFactoryInterface() {
        return javax.persistence.EntityManagerFactory.class;
    }

    public Class<? extends EntityManager> getEntityManagerInterface() {
        return javax.persistence.EntityManager.class;
    }

    public JpaDialect getJpaDialect() {
        return null;
    }

    public Map<String, ?> getJpaPropertyMap() {
        return null;
    }

    public Map<String, ?> getJpaPropertyMap(PersistenceUnitInfo persistenceUnitInfo) {
        return null;
    }

    public void postProcessEntityManagerFactory(EntityManagerFactory entitymanagerfactory) {
    }

}
