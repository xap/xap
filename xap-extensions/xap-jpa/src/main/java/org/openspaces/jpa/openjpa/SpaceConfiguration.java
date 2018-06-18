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

package org.openspaces.jpa.openjpa;

import com.gigaspaces.client.transaction.ITransactionManagerProvider;
import com.gigaspaces.client.transaction.ITransactionManagerProvider.TransactionManagerType;
import com.gigaspaces.client.transaction.TransactionManagerConfiguration;
import com.gigaspaces.client.transaction.TransactionManagerProviderFactory;
import com.gigaspaces.internal.client.spaceproxy.ISpaceProxy;
import com.j_spaces.core.IJSpace;
import com.j_spaces.core.client.ReadModifiers;

import net.jini.core.transaction.server.TransactionManager;

import org.apache.openjpa.conf.OpenJPAConfigurationImpl;
import org.apache.openjpa.kernel.BrokerImpl;
import org.apache.openjpa.lib.conf.Value;
import org.openspaces.core.GigaSpace;
import org.openspaces.core.GigaSpaceConfigurer;
import org.openspaces.core.space.UrlSpaceConfigurer;
import org.openspaces.remoting.ExecutorRemotingProxyConfigurer;
import org.openspaces.remoting.scripting.ScriptingExecutor;

/**
 * Holds OpenJPA's configuration properties & GigaSpaces resources. OpenJPA keeps a single instance
 * of this class.
 *
 * @author idan
 * @since 8.0
 */
public class SpaceConfiguration extends OpenJPAConfigurationImpl {

    private static final long serialVersionUID = -61291372655467023L;

    private IJSpace _space;
    private ITransactionManagerProvider _transactionManagerProvider;
    private int _readModifier;
    private ScriptingExecutor<?> _scriptingExecutor;

    public SpaceConfiguration() {
        super();
        supportedOptions().add(OPTION_OPTIMISTIC);
        supportedOptions().add(OPTION_INC_FLUSH);
        // Default transaction timeout
        setLockTimeout(0);
        setLockManager("none");
        setDynamicEnhancementAgent(false);
        _readModifier = ReadModifiers.REPEATABLE_READ;
    }

    public void initialize() {
        // Set a space proxy using the provided connection url
        // if the space was injected - do nothing.
        if (_space == null) {
            Value configurationValue = getValue("ConnectionFactory");
            if (configurationValue != null && configurationValue.get() != null)
                _space = (IJSpace) configurationValue.get();
            else
                _space = new UrlSpaceConfigurer(getConnectionURL()).space();

            //if configured to use optimistic locking - set it on the space proxy
            if (getOptimistic())
                _space.setOptimisticLocking(true);
        }

        // Create a transaction manager
        TransactionManagerConfiguration configuration = new TransactionManagerConfiguration(TransactionManagerType.DISTRIBUTED);
        try {
            _transactionManagerProvider = TransactionManagerProviderFactory.newInstance(_space, configuration);
        } catch (Exception e) {
            throw new RuntimeException(e.getMessage(), e);
        }

        // Set read lock level (modifier)
        if (getReadLockLevel().equals("write")) {
            _readModifier = ReadModifiers.EXCLUSIVE_READ_LOCK;
        } else if (getOptimistic()) {
            //use read committed isolation level for optimistic locking - to avoid object locking on read operation
            _readModifier = ReadModifiers.READ_COMMITTED;
        } else {
            _readModifier = ReadModifiers.REPEATABLE_READ;
        }
    }

    public IJSpace getSpace() {
        return _space;
    }

    public TransactionManager getTransactionManager() {
        return _transactionManagerProvider.getTransactionManager();
    }

    public int getReadModifier() {
        return _readModifier;
    }

    @Override
    public void setConnectionFactory(Object space) {
        _space = (IJSpace) space;
    }

    /**
     * Create a new broker instance.
     */
    @Override
    public BrokerImpl newBrokerInstance(String user, String pass) {
        return new Broker();
    }

    /**
     * Gets GigaSpaces {@link ScriptingExecutor} proxy used for executing a dynamic {@link
     * org.openspaces.remoting.scripting.Script}.
     *
     * @return {@link ScriptingExecutor} proxy.
     */
    public ScriptingExecutor<?> getScriptingExecutorProxy() {
        if (_scriptingExecutor == null) {
            initializeScriptingExecutorProxy();
        }
        return _scriptingExecutor;
    }

    @SuppressWarnings("rawtypes")
    private synchronized void initializeScriptingExecutorProxy() {
        if (_scriptingExecutor == null) {
            boolean clustered = ((ISpaceProxy) getSpace()).isClustered();
            GigaSpace gigaSpace = new GigaSpaceConfigurer(getSpace()).clustered(clustered).gigaSpace();
            _scriptingExecutor = new ExecutorRemotingProxyConfigurer<ScriptingExecutor>(gigaSpace, ScriptingExecutor.class).proxy();
        }
    }


}
