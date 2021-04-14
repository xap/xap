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


package org.openspaces.core.exception;

import com.gigaspaces.client.protective.ProtectiveModeException;
import com.gigaspaces.security.SecurityException;
import com.j_spaces.core.MemoryShortageException;
import com.j_spaces.core.client.CacheException;
import com.j_spaces.core.client.CacheTimeoutException;
import com.j_spaces.core.client.DuplicateIndexValueException;
import com.j_spaces.core.client.EntryVersionConflictException;
import com.j_spaces.core.client.OperationTimeoutException;
import com.j_spaces.core.client.sql.SQLQueryException;
import com.j_spaces.core.exception.ClosedResourceException;

import net.jini.core.transaction.TransactionException;

import org.openspaces.core.*;
import org.springframework.dao.DataAccessException;

import java.rmi.RemoteException;

/**
 * The default exception translator.
 *
 * @author kimchy
 * @see org.openspaces.core.GigaSpace
 * @see org.openspaces.core.DefaultGigaSpace
 * @see org.openspaces.core.GigaSpaceFactoryBean
 */
public class DefaultExceptionTranslator implements ExceptionTranslator {

    /**
     * Translates general JavaSpaces and Jini exceptions into a DataAccess exception.
     */
    public DataAccessException translate(Throwable e) {
        DataAccessException dae = internalTranslate(e);
        if (dae != null) {
            return dae;
        }
        if (e instanceof ProtectiveModeException)
            throw new ProtectiveModeException(e.getMessage(), e);
        if (e instanceof RuntimeException) {
            throw (RuntimeException) e;
        }
        return new UncategorizedSpaceException(e.getMessage(), e);
    }

    public DataAccessException translateNoUncategorized(Throwable e) {
        return internalTranslate(e);
    }

    private DataAccessException internalTranslate(Throwable e) {
        if (e == null) {
            return null;
        }
        if (e instanceof DuplicateIndexValueException) {
            return new UniqueConstraintViolationException(e.toString(), e);
        }

        if (e instanceof DataAccessException) {
            return (DataAccessException) e;
        }

        if (e instanceof org.springframework.transaction.TransactionException) {
            return new TransactionDataAccessException((org.springframework.transaction.TransactionException) e);
        }

        if (e instanceof CacheException) {
            if (e instanceof CacheTimeoutException) {
                return new SpaceTimeoutException(e.getMessage(), e);
            }
            Throwable cause = e.getCause();
            if (cause != null)
                return internalTranslate(cause);
            return null;
        }

        if (e instanceof InterruptedException) {
            return new SpaceInterruptedException(e.getMessage(), (InterruptedException) e);
        }

        if (e instanceof net.jini.space.InternalSpaceException) {
            if (e.getCause() != null) {
                DataAccessException dae = internalTranslate(e.getCause());
                if (dae != null) {
                    return dae;
                }
            }
            return new InternalSpaceException((net.jini.space.InternalSpaceException) e);
        }

        if (e instanceof RemoteException) {
            RemoteException remoteException = (RemoteException) e;
            if (remoteException.getCause() != null) {
                DataAccessException dae = internalTranslate(remoteException.getCause());
                if (dae != null) {
                    return dae;
                }
            }
            return new RemoteDataAccessException(remoteException);
        }

        if (e instanceof ClosedResourceException) {
            return new SpaceClosedException((ClosedResourceException) e);
        }

        if (e instanceof com.j_spaces.core.exception.SpaceUnavailableException) {
            return new SpaceUnavailableException((com.j_spaces.core.exception.SpaceUnavailableException) e);
        }

        if (e instanceof com.gigaspaces.internal.metadata.converter.ConversionException) {
            return new ObjectConversionException((com.gigaspaces.internal.metadata.converter.ConversionException) e);
        }

        if (e instanceof com.gigaspaces.internal.server.space.tiered_storage.error.TieredStorageMetadataException) {
            return new TieredStorageMetadataException(e.getMessage(), e.getCause());
        }

        if (e instanceof com.gigaspaces.metadata.SpaceMetadataException) {
            return new SpaceMetadataException(e.getMessage(), e.getCause());
        }

        if (e instanceof com.gigaspaces.internal.server.space.tiered_storage.error.TieredStorageOperationException) {
            return new TieredStorageOperationException(e.getMessage(), e.getCause());
        }

        if (e instanceof com.gigaspaces.client.WriteMultipleException) {
            return new WriteMultipleException((com.gigaspaces.client.WriteMultipleException) e, this);
        }

        if (e instanceof com.gigaspaces.client.ClearException) {
            return new ClearException((com.gigaspaces.client.ClearException) e, this);
        }
        if (e instanceof com.gigaspaces.client.ReadMultipleException) {
            return new ReadMultipleException((com.gigaspaces.client.ReadMultipleException) e, this);
        }
        if (e instanceof com.gigaspaces.client.ReadByIdsException) {
            return new ReadByIdsException((com.gigaspaces.client.ReadByIdsException) e, this);
        }
        if (e instanceof com.gigaspaces.client.TakeByIdsException) {
            return new TakeByIdsException((com.gigaspaces.client.TakeByIdsException) e, this);
        }
        if (e instanceof com.gigaspaces.client.TakeMultipleException) {
            return new TakeMultipleException((com.gigaspaces.client.TakeMultipleException) e, this);
        }

        if (e instanceof SQLQueryException) {
            return new BadSqlQueryException((SQLQueryException) e);
        }

        // UnusableEntryException and its subclasses
        if (e instanceof EntryVersionConflictException) {
            return new SpaceOptimisticLockingFailureException((EntryVersionConflictException) e);
        }
        if (e instanceof com.j_spaces.core.client.EntryNotInSpaceException) {
            return new EntryNotInSpaceException((com.j_spaces.core.client.EntryNotInSpaceException) e);
        }
        if (e instanceof com.j_spaces.core.client.EntryAlreadyInSpaceException) {
            return new EntryAlreadyInSpaceException((com.j_spaces.core.client.EntryAlreadyInSpaceException) e);
        }
        if (e instanceof com.j_spaces.core.InvalidFifoTemplateException) {
            return new InvalidFifoTemplateException((com.j_spaces.core.InvalidFifoTemplateException) e);
        }
        if (e instanceof com.j_spaces.core.InvalidFifoClassException) {
            return new InvalidFifoClassException((com.j_spaces.core.InvalidFifoClassException) e);
        }
        if (e instanceof net.jini.core.entry.UnusableEntryException) {
            return new UnusableEntryException((net.jini.core.entry.UnusableEntryException) e);
        }

        // handle transaction exceptions
        if (e instanceof TransactionException) {
            if (e.getMessage().indexOf("not active") > -1) {
                return new InactiveTransactionException((TransactionException) e);
            }
            if (e.getMessage().indexOf("wrong") > -1) {
                return new InvalidTransactionUsageException((TransactionException) e);
            }
            return new TransactionDataAccessException((TransactionException) e);
        }
        if (e instanceof com.j_spaces.core.TransactionNotActiveException) {
            return new InactiveTransactionException((com.j_spaces.core.TransactionNotActiveException) e);
        }

        if (e instanceof com.j_spaces.core.EntrySerializationException) {
            return new EntrySerializationException((com.j_spaces.core.EntrySerializationException) e);
        }

        if (e instanceof MemoryShortageException) {
            return new SpaceMemoryShortageException((MemoryShortageException) e);
        }

        if (e instanceof com.gigaspaces.client.EntryLockedException) {
            return new EntryLockedException(e.getMessage(), e);
        }

        if (e instanceof OperationTimeoutException) {
            return new UpdateOperationTimeoutException((OperationTimeoutException) e);
        }

        if (e instanceof SecurityException) {
            return new SecurityAccessException(e);
        }

        if (e instanceof com.gigaspaces.cluster.replication.RedoLogCapacityExceededException) {
            return new RedoLogCapacityExceededException((com.gigaspaces.cluster.replication.RedoLogCapacityExceededException) e);
        }

        if (e instanceof com.gigaspaces.client.ResourceCapacityExceededException) {
            return new ResourceCapacityExceededException((com.gigaspaces.client.ResourceCapacityExceededException) e);
        }

        if (e instanceof com.gigaspaces.client.ChangeException) {
            return new ChangeException((com.gigaspaces.client.ChangeException) e, this);
        }

        if (e instanceof com.gigaspaces.cluster.replication.ConsistencyLevelViolationException) {
            return new ConsistencyLevelViolationException((com.gigaspaces.cluster.replication.ConsistencyLevelViolationException) e);
        }
        return null;
    }
}
