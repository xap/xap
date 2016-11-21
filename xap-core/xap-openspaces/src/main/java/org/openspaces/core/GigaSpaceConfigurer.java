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
import org.openspaces.core.space.SpaceConfigurer;
import org.openspaces.core.transaction.TransactionProvider;
import org.springframework.core.Constants;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.TransactionDefinition;

/**
 * A simple programmatic configurer for {@link org.openspaces.core.GigaSpace} instance wrapping the
 * {@link org.openspaces.core.GigaSpaceFactoryBean}.
 *
 * <p>Usage example:
 * <pre>
 * SpaceConfigurer spaceConfigurer = new EmbeddedSpaceConfigurer("mySpace");
 * GigaSpace gigaSpace = new GigaSpaceConfigurer(spaceConfigurer).create();
 * ...
 * spaceConfigurer.close();
 * </pre>
 *
 * @author kimchy
 */
public class GigaSpaceConfigurer {
    /**
     * Constants instance for TransactionDefinition
     */
    private static final Constants constants = new Constants(TransactionDefinition.class);

    /**
     * Prefix for the isolation constants defined in TransactionDefinition
     */
    public static final String PREFIX_ISOLATION = "ISOLATION_";

    private DefaultGigaSpace gigaSpace;

    private IJSpace space;
    private String name;
    private Boolean clustered;
    private ExceptionTranslator exTranslator;
    private TransactionProvider txProvider;
    private PlatformTransactionManager transactionManager;
    private int defaultIsolationLevel = TransactionDefinition.ISOLATION_DEFAULT;
    private long defaultReadTimeout = 0;
    private long defaultTakeTimeout = 0;
    private long defaultWriteLease = Long.MAX_VALUE;
    private WriteModifiers defaultWriteModifiers;
    private ReadModifiers defaultReadModifiers;
    private TakeModifiers defaultTakeModifiers;
    private ClearModifiers defaultClearModifiers;
    private CountModifiers defaultCountModifiers;
    private ChangeModifiers defaultChangeModifiers;

    /**
     * Constructs a new configurer based on the Space.
     */
    public GigaSpaceConfigurer(IJSpace space) {
        space(space);
    }

    /**
     * Constructs a new configurer based on the Space.
     */
    public GigaSpaceConfigurer(SpaceConfigurer configurer) {
        this(configurer.space());
    }

    /**
     * For internal usage only
     * @see GigaSpaceFactoryBean
     */
    protected GigaSpaceConfigurer() {
    }

    /**
     * For internal usage only
     * @see GigaSpaceFactoryBean
     */
    protected void space(IJSpace space) {
        this.space = space;
    }

    public IJSpace getSpace() {
        return space;
    }

    /**
     * Sets the name of the GigaSpace instance which will be created. If not specified, the space name will be used.
     * @param name Name of the GigaSpace instance.
     */
    public GigaSpaceConfigurer name(String name) {
        this.name = name;
        return this;
    }

    public String getName() {
        return name;
    }

    /**
     * Sets the transaction provider that will be used by the created {@link
     * org.openspaces.core.GigaSpace}. This is an optional parameter and defaults to {@link
     * org.openspaces.core.transaction.DefaultTransactionProvider}.
     *
     * @param txProvider The transaction provider to use
     */
    public GigaSpaceConfigurer txProvider(TransactionProvider txProvider) {
        this.txProvider = txProvider;
        return this;
    }

    public TransactionProvider getTxProvider() {
        return txProvider;
    }

    /**
     * Sets the exception translator that will be used by the created {@link
     * org.openspaces.core.GigaSpace}. This is an optional parameter and defaults to {@link
     * org.openspaces.core.exception.DefaultExceptionTranslator}.
     *
     * @param exTranslator The exception translator to use
     */
    public GigaSpaceConfigurer exTranslator(ExceptionTranslator exTranslator) {
        this.exTranslator = exTranslator;
        return this;
    }

    public ExceptionTranslator getExTranslator() {
        return exTranslator;
    }

    /**
     * Sets the cluster flag controlling if this {@link org.openspaces.core.GigaSpace} will work
     * with a clustered view of the space or directly with a cluster member. By default if this flag
     * is not set it will be set automatically by this factory. It will be set to <code>false</code>
     * if the space is an embedded one AND the space is not a local cache proxy. It will be set to
     * <code>true</code> otherwise (i.e. the space is not an embedded space OR the space is a local
     * cache proxy).
     *
     * @param clustered If the {@link org.openspaces.core.GigaSpace} is going to work with a
     *                  clustered view of the space or directly with a cluster member
     */
    public GigaSpaceConfigurer clustered(boolean clustered) {
        this.clustered = clustered;
        return this;
    }

    public Boolean getClustered() {
        return clustered;
    }

    /**
     * Sets the default read timeout for {@link org.openspaces.core.GigaSpace#read(Object)} and
     * {@link org.openspaces.core.GigaSpace#readIfExists(Object)} operations. Default to 0.
     */
    public GigaSpaceConfigurer defaultReadTimeout(long defaultReadTimeout) {
        this.defaultReadTimeout = defaultReadTimeout;
        return this;
    }

    public long getDefaultReadTimeout() {
        return defaultReadTimeout;
    }

    /**
     * Sets the default take timeout for {@link org.openspaces.core.GigaSpace#take(Object)} and
     * {@link org.openspaces.core.GigaSpace#takeIfExists(Object)} operations. Default to 0.
     */
    public GigaSpaceConfigurer defaultTakeTimeout(long defaultTakeTimeout) {
        this.defaultTakeTimeout = defaultTakeTimeout;
        return this;
    }

    public long getDefaultTakeTimeout() {
        return defaultTakeTimeout;
    }

    /**
     * Sets the default write lease for {@link org.openspaces.core.GigaSpace#write(Object)}
     * operation. Default to {@link net.jini.core.lease.Lease#FOREVER}.
     */
    public GigaSpaceConfigurer defaultWriteLease(long defaultWriteLease) {
        this.defaultWriteLease = defaultWriteLease;
        return this;
    }

    public long getDefaultWriteLease() {
        return defaultWriteLease;
    }

    /**
     * Set the default isolation level. Must be one of the isolation constants in the
     * TransactionDefinition interface. Default is ISOLATION_DEFAULT.
     *
     * @throws IllegalArgumentException if the supplied value is not one of the <code>ISOLATION_</code>
     *                                  constants
     * @see org.springframework.transaction.TransactionDefinition#ISOLATION_DEFAULT
     */
    public GigaSpaceConfigurer defaultIsolationLevel(int defaultIsolationLevel) {
        if (!constants.getValues(PREFIX_ISOLATION).contains(Integer.valueOf(defaultIsolationLevel))) {
            throw new IllegalArgumentException("Only values of isolation constants allowed");
        }
        this.defaultIsolationLevel = defaultIsolationLevel;
        return this;
    }

    public int getDefaultIsolationLevel() {
        return defaultIsolationLevel;
    }

    /**
     * Set the default isolation level. Must be one of the isolation constants in the
     * TransactionDefinition interface. Default is ISOLATION_DEFAULT.
     *
     * @throws IllegalArgumentException if the supplied value is not one of the <code>ISOLATION_</code>
     *                                  constants
     * @see org.springframework.transaction.TransactionDefinition#ISOLATION_DEFAULT
     */
    public GigaSpaceConfigurer defaultIsolationLevel(String name) {
        if (name == null || !name.startsWith(PREFIX_ISOLATION)) {
            throw new IllegalArgumentException("Only isolation constants allowed");
        }
        defaultIsolationLevel(constants.asNumber(name).intValue());
        return this;
    }

    /**
     * Set the default {@link WriteModifiers} to be used for write operations on the {@link
     * GigaSpace} instance. Defaults to {@link WriteModifiers#UPDATE_OR_WRITE}
     *
     * @param defaultWriteModifiers The default write modifiers.
     * @see WriteModifiers
     */
    public GigaSpaceConfigurer defaultWriteModifiers(WriteModifiers defaultWriteModifiers) {
        this.defaultWriteModifiers = defaultWriteModifiers;
        return this;
    }

    public WriteModifiers getDefaultWriteModifiers() {
        return defaultWriteModifiers;
    }

    /**
     * Set the default {@link ReadModifiers} to be used for read operations on the {@link GigaSpace}
     * instance. Defaults to {@link ReadModifiers#READ_COMMITTED}
     *
     * @param defaultReadModifiers The default read modifiers.
     * @see ReadModifiers
     */
    public GigaSpaceConfigurer defaultReadModifiers(ReadModifiers defaultReadModifiers) {
        this.defaultReadModifiers = defaultReadModifiers;
        return this;
    }

    public ReadModifiers getDefaultReadModifiers() {
        return defaultReadModifiers;
    }

    /**
     * Set the default {@link TakeModifiers} to be used for take operations on the {@link GigaSpace}
     * instance. Defaults to {@link TakeModifiers#NONE}
     *
     * @param defaultTakeModifiers The default take modifiers.
     * @see TakeModifiers
     */
    public GigaSpaceConfigurer defaultTakeModifiers(TakeModifiers defaultTakeModifiers) {
        this.defaultTakeModifiers = defaultTakeModifiers;
        return this;
    }

    public TakeModifiers getDefaultTakeModifiers() {
        return defaultTakeModifiers;
    }

    /**
     * Set the default {@link CountModifiers} to be used for count operations on the {@link
     * GigaSpace} instance. Defaults to {@link CountModifiers#NONE}
     *
     * @param defaultCountModifiers The default count modifiers.
     * @see CountModifiers
     */
    public GigaSpaceConfigurer defaultCountModifiers(CountModifiers defaultCountModifiers) {
        this.defaultCountModifiers = defaultCountModifiers;
        return this;
    }

    public CountModifiers getDefaultCountModifiers() {
        return defaultCountModifiers;
    }

    /**
     * Set the default {@link ClearModifiers} to be used for clear operations on the {@link
     * GigaSpace} instance. Defaults to {@link ClearModifiers#NONE}
     *
     * @param defaultClearModifiers The default clear modifiers.
     * @see ClearModifiers
     */
    public GigaSpaceConfigurer defaultClearModifiers(ClearModifiers defaultClearModifiers) {
        this.defaultClearModifiers = defaultClearModifiers;
        return this;
    }

    public ClearModifiers getDefaultClearModifiers() {
        return defaultClearModifiers;
    }

    /**
     * Set the default {@link ChangeModifiers} to be used for change operations on the {@link
     * GigaSpace} instance. Defaults to {@link ChangeModifiers#NONE}
     *
     * @param defaultChangeModifiers The default change modifiers.
     * @see ChangeModifiers
     */
    public GigaSpaceConfigurer defaultChangeModifiers(ChangeModifiers defaultChangeModifiers) {
        this.defaultChangeModifiers = defaultChangeModifiers;
        return this;
    }


    public ChangeModifiers getDefaultChangeModifiers() {
        return defaultChangeModifiers;
    }

    /**
     * Set the transaction manager to enable transactional operations. Can be <code>null</code>
     * if transactional support is not required or the default space is used as a transactional
     * context.
     */
    public GigaSpaceConfigurer transactionManager(PlatformTransactionManager transactionManager) {
        this.transactionManager = transactionManager;
        return this;
    }

    public PlatformTransactionManager getTransactionManager() {
        return transactionManager;
    }

    /**
     * Creates a new {@link org.openspaces.core.GigaSpace} instance if non already created.
     */
    public GigaSpace create() {
        if (gigaSpace == null) {
            gigaSpace = initialize();
        }
        return gigaSpace;
    }

    protected void close() throws Exception {
        if (gigaSpace != null) {
            gigaSpace.close();
        }
    }

    protected GigaSpace getGigaSpaceIfInitialized() {
        return gigaSpace;
    }

    /**
     * Creates a new {@link org.openspaces.core.GigaSpace} instance if non already created.
     *
     * @see #create()
     */
    public GigaSpace gigaSpace() {
        return create();
    }

    protected DefaultGigaSpace initialize() {
        return new DefaultGigaSpace(this);
    }
}
