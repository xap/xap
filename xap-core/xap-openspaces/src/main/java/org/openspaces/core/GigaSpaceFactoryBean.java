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

package org.openspaces.core;

import com.gigaspaces.client.ChangeModifiers;
import com.gigaspaces.client.ClearModifiers;
import com.gigaspaces.client.CountModifiers;
import com.gigaspaces.client.ReadModifiers;
import com.gigaspaces.client.TakeModifiers;
import com.gigaspaces.client.WriteModifiers;
import com.j_spaces.core.IJSpace;

import org.openspaces.core.exception.ExceptionTranslator;
import org.openspaces.core.transaction.TransactionProvider;
import org.springframework.beans.factory.BeanNameAware;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.beans.factory.FactoryBean;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.transaction.PlatformTransactionManager;

/**
 * <p>A factory bean creating {@link org.openspaces.core.GigaSpace GigaSpace} implementation. The
 * implementation created is {@link org.openspaces.core.DefaultGigaSpace DefaultGigaSpace} which
 * allows for pluggable {@link com.j_spaces.core.IJSpace IJSpace}, {@link
 * org.openspaces.core.transaction.TransactionProvider TransactionProvider}, and {@link
 * org.openspaces.core.exception.ExceptionTranslator ExceptionTranslator}.
 *
 * <p>The factory requires an {@link com.j_spaces.core.IJSpace IJSpace} which can be either directly
 * acquired or build using one of the several space factory beans provided in
 * <code>org.openspaces.core.space</code>.
 *
 * <p>The factory accepts an optional {@link org.openspaces.core.transaction.TransactionProvider
 * TransactionProvider} which defaults to {@link org.openspaces.core.transaction.DefaultTransactionProvider
 * DefaultTransactionProvider}. The transactional context used is based on {@link
 * #setTransactionManager(org.springframework.transaction.PlatformTransactionManager)} and if no
 * transaction manager is provided, will use the space as the context.
 *
 * <p>When using {@link org.openspaces.core.transaction.manager.LocalJiniTransactionManager} there
 * is no need to pass the transaction manager to this factory, since both by default will use the
 * space as the transactional context. When working with {@link org.openspaces.core.transaction.manager.LookupJiniTransactionManager}
 * (which probably means Mahalo and support for more than one space as transaction resources) the
 * transaction manager should be provided to this class.
 *
 * <p>The factory accepts an optional {@link org.openspaces.core.exception.ExceptionTranslator
 * ExceptionTranslator} which defaults to {@link org.openspaces.core.exception.DefaultExceptionTranslator
 * DefaultExceptionTranslator}.
 *
 * <p>A clustered flag allows to control if this GigaSpace instance will work against a clustered
 * view of the space or directly against a clustered member. This flag has no affect when not
 * working in a clustered mode (partitioned or primary/backup). By default if this flag is not set
 * it will be set automatically by this factory. It will be set to <code>true</code> if the space is
 * an embedded one AND the space is not a local cache proxy. It will be set to <code>false</code>
 * otherwise (i.e. the space is not an embedded space OR the space is a local cache proxy). A local
 * cache proxy is an <code>IJSpace</code> that is injected using {@link
 * #setSpace(com.j_spaces.core.IJSpace)} and was created using either {@link
 * org.openspaces.core.space.cache.LocalViewSpaceFactoryBean} or {@link
 * org.openspaces.core.space.cache.LocalCacheSpaceFactoryBean}.
 *
 * <p>The factory allows to set the default read/take timeout and write lease when using the same
 * operations without the relevant parameters.
 *
 * <p>The factory also allows to set the default isolation level for read operations that will be
 * performed by {@link org.openspaces.core.GigaSpace} API. The isolation level can be set either
 * using {@link #setDefaultIsolationLevel(int)} or {@link #setDefaultIsolationLevelName(String)}.
 * Note, this setting will apply when not working under Spring declarative transactions or when
 * using Spring declarative transaction with the default isolation level ({@link
 * org.springframework.transaction.TransactionDefinition#ISOLATION_DEFAULT}).
 *
 * @author kimchy
 * @see org.openspaces.core.GigaSpace
 * @see org.openspaces.core.DefaultGigaSpace
 * @see org.openspaces.core.transaction.TransactionProvider
 * @see org.openspaces.core.exception.ExceptionTranslator
 * @see org.openspaces.core.transaction.manager.AbstractJiniTransactionManager
 */
public class GigaSpaceFactoryBean implements InitializingBean, DisposableBean, FactoryBean, BeanNameAware {

    /**
     * Prefix for the isolation constants defined in TransactionDefinition
     */
    public static final String PREFIX_ISOLATION = GigaSpaceConfigurer.PREFIX_ISOLATION;

    private final GigaSpaceConfigurer configurer = new GigaSpaceConfigurer();

    /**
     * <p>Sets the space that will be used by the created {@link org.openspaces.core.GigaSpace}.
     * This is a required parameter to the factory.
     *
     * @param space The space used
     */
    public void setSpace(IJSpace space) {
        configurer.space(space);
    }

    /**
     * <p>Sets the transaction provider that will be used by the created {@link
     * org.openspaces.core.GigaSpace}. This is an optional parameter and defaults to {@link
     * org.openspaces.core.transaction.DefaultTransactionProvider}.
     *
     * @param txProvider The transaction provider to use
     */
    public void setTxProvider(TransactionProvider txProvider) {
        configurer.txProvider(txProvider);
    }

    /**
     * <p>Sets the exception translator that will be used by the created {@link
     * org.openspaces.core.GigaSpace}. This is an optional parameter and defaults to {@link
     * org.openspaces.core.exception.DefaultExceptionTranslator}.
     *
     * @param exTranslator The exception translator to use
     */
    public void setExTranslator(ExceptionTranslator exTranslator) {
        configurer.exTranslator(exTranslator);
    }

    /**
     * <p>Sets the cluster flag controlling if this {@link org.openspaces.core.GigaSpace} will work
     * with a clustered view of the space or directly with a cluster member. By default if this flag
     * is not set it will be set automatically by this factory. It will be set to <code>false</code>
     * if the space is an embedded one AND the space is not a local cache proxy. It will be set to
     * <code>true</code> otherwise (i.e. the space is not an embedded space OR the space is a local
     * cache proxy).
     *
     * @param clustered If the {@link org.openspaces.core.GigaSpace} is going to work with a
     *                  clustered view of the space or directly with a cluster member
     */
    public void setClustered(boolean clustered) {
        configurer.clustered(clustered);
    }

    /**
     * <p>Sets the default read timeout for {@link org.openspaces.core.GigaSpace#read(Object)} and
     * {@link org.openspaces.core.GigaSpace#readIfExists(Object)} operations. Default to 0.
     */
    public void setDefaultReadTimeout(long defaultReadTimeout) {
        configurer.defaultReadTimeout(defaultReadTimeout);
    }

    /**
     * <p>Sets the default take timeout for {@link org.openspaces.core.GigaSpace#take(Object)} and
     * {@link org.openspaces.core.GigaSpace#takeIfExists(Object)} operations. Default to 0.
     */
    public void setDefaultTakeTimeout(long defaultTakeTimeout) {
        configurer.defaultTakeTimeout(defaultTakeTimeout);
    }

    /**
     * <p>Sets the default write lease for {@link org.openspaces.core.GigaSpace#write(Object)}
     * operation. Default to {@link net.jini.core.lease.Lease#FOREVER}.
     */
    public void setDefaultWriteLease(long defaultWriteLease) {
        configurer.defaultWriteLease(defaultWriteLease);
    }

    /**
     * Set the default isolation level by the name of the corresponding constant in
     * TransactionDefinition, e.g. "ISOLATION_DEFAULT".
     *
     * @param constantName name of the constant
     * @throws IllegalArgumentException if the supplied value is not resolvable to one of the
     *                                  <code>ISOLATION_</code> constants or is <code>null</code>
     * @see #setDefaultIsolationLevel(int)
     * @see org.springframework.transaction.TransactionDefinition#ISOLATION_DEFAULT
     */
    public final void setDefaultIsolationLevelName(String constantName) throws IllegalArgumentException {
        configurer.defaultIsolationLevel(constantName);
    }

    /**
     * Set the default isolation level. Must be one of the isolation constants in the
     * TransactionDefinition interface. Default is ISOLATION_DEFAULT.
     *
     * @throws IllegalArgumentException if the supplied value is not one of the <code>ISOLATION_</code>
     *                                  constants
     * @see org.springframework.transaction.TransactionDefinition#ISOLATION_DEFAULT
     */
    public void setDefaultIsolationLevel(int defaultIsolationLevel) {
        configurer.defaultIsolationLevel(defaultIsolationLevel);
    }

    /**
     * Set the default {@link WriteModifiers} to be used for write operations on the {@link
     * GigaSpace} instance. Defaults to {@link WriteModifiers#UPDATE_OR_WRITE}
     *
     * @param defaultWriteModifiers The default write modifiers.
     * @see WriteModifiers
     */
    public void setDefaultWriteModifiers(WriteModifiers[] defaultWriteModifiers) {
        if (defaultWriteModifiers != null && defaultWriteModifiers.length != 0) {
            WriteModifiers modifiers = defaultWriteModifiers[0];
            for (int i=1 ; i < defaultWriteModifiers.length ; i++)
                modifiers = modifiers.add(defaultWriteModifiers[i]);
            configurer.defaultWriteModifiers(modifiers);
        }
    }

    /**
     * Set the default {@link ReadModifiers} to be used for read operations on the {@link GigaSpace}
     * instance. Defaults to {@link ReadModifiers#READ_COMMITTED}
     *
     * @param defaultReadModifiers The default read modifiers.
     * @see ReadModifiers
     */
    public void setDefaultReadModifiers(ReadModifiers[] defaultReadModifiers) {
        if (defaultReadModifiers != null && defaultReadModifiers.length != 0) {
            ReadModifiers modifiers = defaultReadModifiers[0];
            for (int i=1 ; i < defaultReadModifiers.length ; i++)
                modifiers = modifiers.add(defaultReadModifiers[i]);
            configurer.defaultReadModifiers(modifiers);
        }
    }

    /**
     * Set the default {@link TakeModifiers} to be used for take operations on the {@link GigaSpace}
     * instance. Defaults to {@link TakeModifiers#NONE}
     *
     * @param defaultTakeModifiers The default take modifiers.
     * @see TakeModifiers
     */
    public void setDefaultTakeModifiers(TakeModifiers[] defaultTakeModifiers) {
        if (defaultTakeModifiers != null && defaultTakeModifiers.length != 0) {
            TakeModifiers modifiers = defaultTakeModifiers[0];
            for (int i=1 ; i < defaultTakeModifiers.length ; i++)
                modifiers = modifiers.add(defaultTakeModifiers[i]);
            configurer.defaultTakeModifiers(modifiers);
        }
    }

    /**
     * Set the default {@link CountModifiers} to be used for count operations on the {@link
     * GigaSpace} instance. Defaults to {@link CountModifiers#NONE}
     *
     * @param defaultCountModifiers The default count modifiers.
     * @see CountModifiers
     */
    public void setDefaultCountModifiers(CountModifiers[] defaultCountModifiers) {
        if (defaultCountModifiers != null && defaultCountModifiers.length != 0) {
            CountModifiers modifiers = defaultCountModifiers[0];
            for (int i=1 ; i < defaultCountModifiers.length ; i++)
                modifiers = modifiers.add(defaultCountModifiers[i]);
            configurer.defaultCountModifiers(modifiers);
        }
    }

    /**
     * Set the default {@link ClearModifiers} to be used for clear operations on the {@link
     * GigaSpace} instance. Defaults to {@link ClearModifiers#NONE}
     *
     * @param defaultClearModifiers The default clear modifiers.
     * @see ClearModifiers
     */
    public void setDefaultClearModifiers(ClearModifiers[] defaultClearModifiers) {
        if (defaultClearModifiers != null && defaultClearModifiers.length != 0) {
            ClearModifiers modifiers = defaultClearModifiers[0];
            for (int i=1 ; i < defaultClearModifiers.length ; i++)
                modifiers = modifiers.add(defaultClearModifiers[i]);
            configurer.defaultClearModifiers(modifiers);
        }
    }

    /**
     * Set the default {@link ChangeModifiers} to be used for change operations on the {@link
     * GigaSpace} instance. Defaults to {@link ChangeModifiers#NONE}
     *
     * @param defaultChangeModifiers The default change modifiers.
     * @see ChangeModifiers
     */
    public void setDefaultChangeModifiers(ChangeModifiers[] defaultChangeModifiers) {
        if (defaultChangeModifiers != null && defaultChangeModifiers.length != 0) {
            ChangeModifiers modifiers = defaultChangeModifiers[0];
            for (int i=1 ; i < defaultChangeModifiers.length ; i++)
                modifiers = modifiers.add(defaultChangeModifiers[i]);
            configurer.defaultChangeModifiers(modifiers);
        }
    }

    /**
     * <p>Set the transaction manager to enable transactional operations. Can be <code>null</code>
     * if transactional support is not required or the default space is used as a transactional
     * context.
     */
    public void setTransactionManager(PlatformTransactionManager transactionManager) {
        configurer.transactionManager(transactionManager);
    }

    public void setBeanName(String beanName) {
        configurer.name(beanName);
    }

    /**
     * Constructs the {@link org.openspaces.core.GigaSpace} instance using the {@link
     * org.openspaces.core.DefaultGigaSpace} implementation. Uses the clustered flag to get a
     * cluster member directly (if set to <code>false</code>) and applies the different defaults).
     */
    public void afterPropertiesSet() {
        configurer.create();
    }

    /**
     * Return {@link org.openspaces.core.GigaSpace} implementation constructed in the {@link
     * #afterPropertiesSet()} phase.
     */
    public Object getObject() {
        return configurer.getGigaSpaceIfInitialized();
    }

    public Class<? extends GigaSpace> getObjectType() {
        GigaSpace gigaSpace = configurer.getGigaSpaceIfInitialized();
        return (gigaSpace == null ? GigaSpace.class : gigaSpace.getClass());
    }

    /**
     * Returns <code>true</code> as this is a singleton.
     */
    public boolean isSingleton() {
        return true;
    }

    /* (non-Javadoc)
     * @see org.springframework.beans.factory.DisposableBean#destroy()
     */
    @Override
    public void destroy() throws Exception {
        configurer.close();
    }
}
