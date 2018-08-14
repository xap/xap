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

/*
 * @(#)WorkerManager.java 1.0  29.08.2004  13:09:39
 */

package com.j_spaces.worker;

import com.gigaspaces.cluster.activeelection.ISpaceComponentsHandler;
import com.gigaspaces.cluster.activeelection.SpaceComponentsInitializeException;
import com.gigaspaces.cluster.activeelection.SpaceInitializationIndicator;
import com.gigaspaces.internal.reflection.ReflectionUtil;
import com.gigaspaces.internal.server.space.SpaceConfigReader;
import com.j_spaces.core.Constants;
import com.j_spaces.core.IJSpace;
import com.j_spaces.kernel.ClassLoaderHelper;

import java.util.Hashtable;
import java.util.Iterator;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * WorkerManager manages users defined workers. During space initialization will be called init()
 * method and on clean or destroy space will be call close() method. Initialize and close workers on
 * user's implementation responsibility.
 *
 * @author Igor Goldenberg
 * @version 4.0
 * @deprecated
 */
@Deprecated
@com.gigaspaces.api.InternalApi
public class WorkerManager implements ISpaceComponentsHandler {
    private static final Logger _logger = Logger.getLogger(com.gigaspaces.logger.Constants.LOGGER_WORKER);

    private final IJSpace workerProxy;
    private final String workerManagerName;
    private final boolean isInterrupt;
    private final Hashtable<String, WorkerInfo> workerTable;
    private boolean isClosed;

    public WorkerManager(IJSpace workerProxy, String fullSpaceName) {
        this.workerProxy = workerProxy;
        this.workerManagerName = workerProxy.getName();

        // Mark this thread as initializer thread to allow space operations during initialization
        SpaceInitializationIndicator.setInitializer();
        try {
            SpaceConfigReader configReader = new SpaceConfigReader(fullSpaceName);
            // is interrupt threads on shutdown, to see the stack trace
            this.isInterrupt = configReader.getBooleanSpaceProperty(Constants.WorkerManager.IS_INTERRUPT_THREADS_ON_SHUTDOWN, "false");
            this.workerTable = buildWorkers(configReader);
        } finally {
            SpaceInitializationIndicator.unsetInitializer();
        }
    }

    public synchronized void shutdown() {
        if (isClosed)
            return;

        for (Iterator<WorkerInfo> iterator = workerTable.values().iterator(); iterator.hasNext(); ) {
            WorkerInfo worker = iterator.next();
            iterator.remove();
            close(worker);
        }

        isClosed = true;

        if (_logger.isLoggable(Level.FINE))
            _logger.fine("WorkerManager: " + workerManagerName + " closed successfully.");
    }

    private void close(WorkerInfo worker) {
        try {
            // interrupt the thread to see the stack trace
            if (isInterrupt)
                worker.getWorkerThread().interrupt();

            worker.getWorker().close();
            if (_logger.isLoggable(Level.FINE))
                _logger.fine("WorkerManager: " + workerManagerName + " Worker: " + worker.getWorkerName() + " closed successfully.");
        } catch (Exception ex) {
            if (_logger.isLoggable(Level.FINE))
                _logger.log(Level.FINE, "WorkerManager: " + workerManagerName + " Failed to close " + worker.getWorkerName(), ex);
        }
    }

    private Hashtable<String, WorkerInfo> buildWorkers(SpaceConfigReader configReader) {

        Hashtable<String, WorkerInfo> result = new Hashtable<String, WorkerInfo>();
        String workerNames = configReader.getSpaceProperty(Constants.WorkerManager.WORKER_NAMES_PROP, null);
        if (workerNames == null) {
            if (_logger.isLoggable(Level.FINE))
                _logger.fine("No worker defined for " + workerManagerName + " space.");
            return result;
        }
        String[] workerArr = workerNames.split(",");

        for (int i = 0; i < workerArr.length; i++) {
            workerArr[i] = workerArr[i].trim();
            String enabled = configReader.getSpaceProperty(Constants.WorkerManager.WORKER_PREFIX + workerArr[i] + '.' + Constants.WorkerManager.WORKER_ENABLED, Constants.WorkerManager.DEFAULT_WORKER_ENABLED);

            /** don't start if enabled=false */
            if (enabled.equalsIgnoreCase(Boolean.FALSE.toString())) {
                if (_logger.isLoggable(Level.FINE))
                    _logger.fine("Worker: " + workerArr[i] + " disabled.");
                continue;
            }

            final String className = configReader.getSpaceProperty(Constants.WorkerManager.WORKER_PREFIX + workerArr[i] + '.' + Constants.WorkerManager.WORKER_CLASSNAME, null);
            String arg = configReader.getSpaceProperty(Constants.WorkerManager.WORKER_PREFIX + workerArr[i] + '.' + Constants.WorkerManager.WORKER_ARG, null);
            String description = configReader.getSpaceProperty(Constants.WorkerManager.WORKER_PREFIX + workerArr[i] + '.' + Constants.WorkerManager.WORKER_DESCRIPTION, null);
            String instancesStr = configReader.getSpaceProperty(Constants.WorkerManager.WORKER_PREFIX + workerArr[i] + '.' + Constants.WorkerManager.WORKER_INSTANCES, Constants.WorkerManager.DEFAULT_WORKER_INSTANCES);
            int instances = Integer.parseInt(instancesStr);
            String activeWhenBackupStr = configReader.getSpaceProperty(Constants.WorkerManager.WORKER_PREFIX + workerArr[i] + '.' + Constants.WorkerManager.WORKER_ACTIVE_WHEN_BACKUP, Constants.WorkerManager.DEFAULT_WORKER_ACTIVE_WHEN_BACKUP);
            boolean startIfPrimaryOnly = !Boolean.parseBoolean(activeWhenBackupStr);
            String shutdownSpaceOnInitFailureStr = configReader.getSpaceProperty(Constants.WorkerManager.WORKER_PREFIX + workerArr[i] + '.' + Constants.WorkerManager.WORKER_SHUTDOWN_ON_INIT_FAILURE, Constants.WorkerManager.DEFAULT_WORKER_SHUTDOWN_ON_INIT_FAILURE);
            boolean shutdownSpaceOnInitFailure = Boolean.parseBoolean(shutdownSpaceOnInitFailureStr);

            for (int j = 0; j < instances; j++) {
                String workerName = workerArr[i] + (j + 1);
                try {
                    WorkerInfo worker = buildWorker(workerName, className, arg, description, startIfPrimaryOnly, shutdownSpaceOnInitFailure);
                    result.put(workerName, worker);
                } catch (Exception ex) {
                    if (_logger.isLoggable(Level.SEVERE))
                        _logger.log(Level.SEVERE, "Failed to configure worker: " + workerName, ex);
                    throw new RuntimeException("WorkerManager: Failed to configure worker [" + workerName + "]", ex);
                }
            }
        }
        if (_logger.isLoggable(Level.FINE))
            _logger.fine("WorkerManager: " + workerManagerName + " initialized successfully.");
        return result;
    }

    private WorkerInfo buildWorker(String workerName, String className, String arg, String desc, boolean startIfPrimaryOnly, boolean shutdownSpaceOnInitFailure)
            throws Exception {
        if (isClosed)
            throw new RuntimeException("Failed to start worker: " + workerName + ".WorkerManager already shutdown.");

        if (_logger.isLoggable(Level.FINE))
            _logger.fine("WorkerManager: " + workerManagerName + " Starting Worker: " + workerName + " Class name: " + className + " arg: " + arg);

        if (className == null) {
            String exMes = "Class name for Worker: " + workerName + " doesn't exist.";
            _logger.severe(exMes);
            throw new IllegalArgumentException(exMes);
        }

        IWorker workerObj;
        try {
            Class workerClass = ClassLoaderHelper.loadClass(className);
            ReflectionUtil.assertIsPublic(workerClass);
            ReflectionUtil.assertHasDefaultConstructor(workerClass);
            workerObj = (IWorker) workerClass.newInstance();
        } catch (IllegalArgumentException ex) {
            throw new IllegalArgumentException("Class name: " + className + " must be public and needs public no-arg constructor.", ex);
        }

        Thread workerThread = new Thread(workerObj);
        workerThread.setName(workerName + "-" + workerProxy.getName());

        WorkerInfo wInfo = new WorkerInfo(workerObj, workerThread, workerName, className, arg, desc, startIfPrimaryOnly, shutdownSpaceOnInitFailure);

        if (_logger.isLoggable(Level.FINE))
            _logger.fine("WorkerManager: " + workerManagerName + " Worker: " + workerName + " initialized successfully.");
        return wInfo;
    }

    @Override
    public void initComponents(final boolean primaryOnly) throws SpaceComponentsInitializeException {
        if (isClosed)
            return;

        for (Iterator<WorkerInfo> iterator = workerTable.values().iterator(); iterator.hasNext(); ) {
            WorkerInfo worker = iterator.next();
            if (worker.isStartIfPrimaryOnly() != primaryOnly)
                continue;

            try {
                worker.getWorker().init(workerProxy, worker.getWorkerName(), worker.getArg());
            } catch (Exception ex) {
                if (_logger.isLoggable(Level.SEVERE))
                    _logger.log(Level.SEVERE, "Failed to initialize worker: " + worker.getWorkerName(), ex);
                if (worker.isShutdownSpaceOnInitFailure())
                    throw new SpaceComponentsInitializeException("Failed to initialize worker - " + worker.getWorkerName(), ex);
                iterator.remove();
                close(worker);
            }
        }
    }

    @Override
    public void startComponents(boolean primaryOnly) {
        if (isClosed)
            return;

        for (WorkerInfo workerInfo : workerTable.values()) {
            if (workerInfo.isStartIfPrimaryOnly() != primaryOnly)
                continue;
            workerInfo.getWorkerThread().start();
        }
    }

    @Override
    public boolean isRecoverySupported() {
        return false;
    }

    @Override
    public void close(boolean primaryOnly) {
        for (Iterator<WorkerInfo> iterator = workerTable.values().iterator(); iterator.hasNext(); ) {
            WorkerInfo worker = iterator.next();
            if (primaryOnly == worker.isStartIfPrimaryOnly()) {
                close(worker);
            }
        }
    }
}
