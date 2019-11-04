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

package com.gigaspaces.internal.sync.mirror;

import com.gigaspaces.internal.cluster.node.impl.processlog.multisourcesinglefile.DistributedTransactionProcessingConfiguration;
import com.gigaspaces.internal.server.space.SpaceConfigReader;
import com.gigaspaces.internal.utils.StringUtils;
import com.gigaspaces.start.SystemInfo;
import com.gigaspaces.start.SystemLocations;
import com.j_spaces.core.Constants;
import com.j_spaces.core.Constants.Mirror;

import java.io.File;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Mirror space configuration. Contains mirror specific configuration located under
 * space-config.mirror. Current possible configuration:
 *
 * <p> space-config.mirror-service.enabled=true<br> space-config.mirror-service.operation-grouping=group-by-space-transaction
 *
 * @author anna
 * @since 7.1
 */
final public class MirrorConfig {
    private static final Logger _logger = Logger.getLogger(com.gigaspaces.logger.Constants.LOGGER_MIRROR_REPLICATION);

    private static final int DEFAULT_NUM_OF_PARTITIONS = 100;
    private static final int DEFAULT_BACKUPS_PER_PARTITION = 1;
    public static final long DIST_TX_WAIT_TIMEOUT = 60000;
    public static final long DIST_TX_WAIT_FOR_OPERATIONS = -1;

    private final BulkOperationGrouping _operationGrouping;
    private final String _importDirectory;
    private final String _clusterName;
    private final int _partitionsCount;
    private final int _backupsPerPartition;

    private final DistributedTransactionProcessingConfiguration _transactionProcessingConfiguration = new DistributedTransactionProcessingConfiguration(DIST_TX_WAIT_TIMEOUT,
            DIST_TX_WAIT_FOR_OPERATIONS);

    public MirrorConfig(SpaceConfigReader configReader) {
        String mirrorPropValue = configReader.getSpaceProperty(
                Mirror.MIRROR_SERVICE_OPERATION_GROUPING_TAG, Mirror.MIRROR_SERVICE_OPERATION_GROUPING_DEFAULT_VALUE);
        _operationGrouping = BulkOperationGrouping.parseOperationGroupingTag(mirrorPropValue);

        String importDirectory = configReader.getSpaceProperty(
                Mirror.MIRROR_IMPORT_DIRECTORY_TAG, null);
        // if space property is a relative path, then its parent directory is work.
        if (importDirectory != null && !(new File(importDirectory)).isAbsolute()) {
            importDirectory = SystemLocations.singleton().work(importDirectory).toAbsolutePath().toString();
        }
        _importDirectory = importDirectory;

        String configLogMessage = "Mirror space configuration:" + StringUtils.NEW_LINE +
                "\t" + Mirror.FULL_MIRROR_SERVICE_OPERATION_GROUPING_TAG + "=" + String.valueOf(mirrorPropValue) +
                "\t" + Mirror.FULL_MIRROR_IMPORT_DIRECTORY_TAG + "=" + String.valueOf(_importDirectory);

        _clusterName = configReader.getSpaceProperty(Mirror.MIRROR_SERVICE_CLUSTER_NAME, null);
        if (_clusterName == null) {
            if (_logger.isLoggable(Level.WARNING)) {
                _logger.warning(StringUtils.NEW_LINE +
                        StringUtils.NEW_LINE +
                        "**************************************************************************************" + StringUtils.NEW_LINE +
                        "Cluster name is missing." + StringUtils.NEW_LINE +
                        "Since version 8.0 a mirror service should be associated to a cluster by configuring the following space property:" + StringUtils.NEW_LINE +
                        "[" + Mirror.FULL_MIRROR_SERVICE_CLUSTER_NAME + "] to point to the cluster name the mirror is part of." + StringUtils.NEW_LINE +
                        "i.e if the cluster name is dataSpace, '" + Mirror.FULL_MIRROR_SERVICE_CLUSTER_NAME + "=dataSpace' " + StringUtils.NEW_LINE +
                        "should be present in the mirror space properties." + StringUtils.NEW_LINE +
                        "Without this configuration split brain consistency protection is weakened!" + StringUtils.NEW_LINE +
                        "**************************************************************************************" + StringUtils.NEW_LINE +
                        StringUtils.NEW_LINE);
            }
        }
        boolean hasPartitionCount = configReader.containsSpaceProperty(Mirror.MIRROR_SERVICE_CLUSTER_PARTITIONS_COUNT);
        _partitionsCount = configReader.getIntSpaceProperty(Mirror.MIRROR_SERVICE_CLUSTER_PARTITIONS_COUNT, String.valueOf(DEFAULT_NUM_OF_PARTITIONS));
        boolean hasMembersPerPartition = configReader.containsSpaceProperty(Mirror.MIRROR_SERVICE_CLUSTER_BACKUPS_PER_PARTITION);
        _backupsPerPartition = configReader.getIntSpaceProperty(Mirror.MIRROR_SERVICE_CLUSTER_BACKUPS_PER_PARTITION, String.valueOf(DEFAULT_BACKUPS_PER_PARTITION));
        if (!hasPartitionCount || !hasMembersPerPartition) {
            if (_logger.isLoggable(Level.WARNING))
                _logger.warning("No cluster configuration was defined for mirror - using default configuration - supports upto " + DEFAULT_NUM_OF_PARTITIONS + " partitions and exactly " + DEFAULT_BACKUPS_PER_PARTITION + " " + (DEFAULT_BACKUPS_PER_PARTITION == 1 ? "backup" : "backups") + " per partition.");
        }

        final String timeoutBeforePartialCommit = configReader.getSpaceProperty(Mirror.MIRROR_DISTRIBUTED_TRANSACTION_TIMEOUT,
                null);
        final String waitForOperationsBeforePartialCommit = configReader.getSpaceProperty(Mirror.MIRROR_DISTRIBUTED_TRANSACTION_WAIT_FOR_OPERATIONS,
                null);
        final String monitorPendingOperationsMemory = configReader.getSpaceProperty(Mirror.MIRROR_DISTRIBUTED_TRANSACTION_MONITOR_PENDING_OPERATIONS_MEMORY,
                null);
        if (timeoutBeforePartialCommit != null)
            _transactionProcessingConfiguration.setTimeoutBeforePartialCommit(Long.parseLong(timeoutBeforePartialCommit));
        if (waitForOperationsBeforePartialCommit != null)
            _transactionProcessingConfiguration.setWaitForOperationsBeforePartialCommit(Integer.parseInt(waitForOperationsBeforePartialCommit));
        if (monitorPendingOperationsMemory != null)
            _transactionProcessingConfiguration.setMonitorPendingOperationsMemory(Boolean.parseBoolean(monitorPendingOperationsMemory));

        configLogMessage +=
                "\t" + Mirror.FULL_MIRROR_SERVICE_CLUSTER_NAME + "=" + _clusterName +
                        "\t" + Mirror.FULL_MIRROR_SERVICE_CLUSTER_PARTITIONS_COUNT + "=" + _partitionsCount +
                        "\t" + Mirror.FULL_MIRROR_SERVICE_CLUSTER_BACKUPS_PER_PARTITION + "=" + _backupsPerPartition +
                        "\t" + Mirror.MIRROR_DISTRIBUTED_TRANSACTION_TIMEOUT + "=" + timeoutBeforePartialCommit +
                        "\t" + Mirror.MIRROR_DISTRIBUTED_TRANSACTION_WAIT_FOR_OPERATIONS + "=" + waitForOperationsBeforePartialCommit +
                        "\t" + Mirror.MIRROR_DISTRIBUTED_TRANSACTION_MONITOR_PENDING_OPERATIONS_MEMORY + "=" + monitorPendingOperationsMemory;

        if (_logger.isLoggable(Level.CONFIG)) {
            _logger.config(configLogMessage);
        }
    }

    public BulkOperationGrouping getOperationGrouping() {
        return _operationGrouping;
    }

    public String getClusterName() {
        return _clusterName;
    }

    public int getBackupsPerPartition() {
        return _backupsPerPartition;
    }

    public int getPartitionsCount() {
        return _partitionsCount;
    }

    public DistributedTransactionProcessingConfiguration getDistributedTransactionProcessingParameters() {
        return _transactionProcessingConfiguration;
    }

    public enum BulkOperationGrouping {
        GROUP_BY_SPACE_TRANSACTION, GROUP_BY_REPLICATION_BULK;

        public static BulkOperationGrouping parseOperationGroupingTag(
                String value) {
            if (Constants.Mirror.GROUP_BY_SPACE_TRANSACTION_TAG_VALUE.equalsIgnoreCase(value))
                return BulkOperationGrouping.GROUP_BY_SPACE_TRANSACTION;
            if (Constants.Mirror.GROUP_BY_REPLICATION_BULK_TAG_VALUE.equalsIgnoreCase(value))
                return BulkOperationGrouping.GROUP_BY_REPLICATION_BULK;

            throw new IllegalArgumentException("Illegal operation-grouping tag value - '" + value + "'. Can be either "
                    + Constants.Mirror.GROUP_BY_SPACE_TRANSACTION_TAG_VALUE
                    + " or "
                    + Constants.Mirror.GROUP_BY_REPLICATION_BULK_TAG_VALUE);
        }
    }
}
