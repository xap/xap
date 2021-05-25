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


package com.j_spaces.core.filters;

import com.gigaspaces.async.AsyncFutureListener;
import com.gigaspaces.executor.SpaceTask;
import com.gigaspaces.logger.Constants;

import net.jini.core.transaction.Transaction;

import java.lang.reflect.Field;
import java.util.Hashtable;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class declares static fields for filter operation codes.
 *
 * @author Asaf Kariv
 * @version 1.0
 */

public class FilterOperationCodes {
    // the following operation codes can be used in security and non-security
    // filters
    /**
     * Before write operation.
     */
    public final static int BEFORE_WRITE = 0;
    /**
     * After write operation.
     */
    public final static int AFTER_WRITE = 1;
    /**
     * Before read operation.
     */
    public final static int BEFORE_READ = 2;
    /**
     * Before take operation.
     */
    public final static int BEFORE_TAKE = 3;
    /**
     * Before notify register operation.
     */
    public final static int BEFORE_NOTIFY = 4;
    /**
     * Before getAdmin operation.
     */
    public final static int BEFORE_GETADMIN = 5;
    /**
     * On setSecurityContext operation.
     *
     * @deprecated see {@link #BEFORE_AUTHENTICATION}
     */
    @Deprecated
    public final static int SET_SECURITY = 6;

    /**
     * Before authentication operation on a secured space.
     */
    public final static int BEFORE_AUTHENTICATION = 6;

    /**
     * Before clean operation.
     */
    public final static int BEFORE_CLEAN_SPACE = 8;
    /**
     * Before update operation.
     */
    public final static int BEFORE_UPDATE = 9;
    /**
     * After update operation.
     */
    public final static int AFTER_UPDATE = 10;
    /**
     * Before readMultiple operation.
     */
    public final static int BEFORE_READ_MULTIPLE = 11;
    /**
     * After readMultiple operation.
     */
    public final static int AFTER_READ_MULTIPLE = 12;
    /**
     * Before takeMultiple operation.
     */
    public final static int BEFORE_TAKE_MULTIPLE = 13;
    /**
     * After takeMultiple operation.
     */
    public final static int AFTER_TAKE_MULTIPLE = 14;
    /**
     * Before notify trigger operation, indicates that a matched notify template was found to the
     * current entry event.
     */
    public final static int BEFORE_NOTIFY_TRIGGER = 15;
    /**
     * After notify trigger operation, indicates that a notify trigger was successful.
     */
    public final static int AFTER_NOTIFY_TRIGGER = 16;
    /**
     * Before all notify trigger operation, indicates that at least one notify template is matched
     * to the current entry event.
     */
    public final static int BEFORE_ALL_NOTIFY_TRIGGER = 17;
    /**
     * After all notify trigger operation, indicates that all notify templates that are matched to
     * the current entry event were triggered and returned or failed.
     *
     * @see net.jini.core.event.RemoteEventListener#notify(net.jini.core.event.RemoteEvent)
     */
    public final static int AFTER_ALL_NOTIFY_TRIGGER = 18;
    /**
     * Before execute operation.
     *
     * @see com.gigaspaces.internal.client.spaceproxy.ISpaceProxy#execute(SpaceTask,
     * java.lang.Object, Transaction, AsyncFutureListener)
     */
    public final static int BEFORE_EXECUTE = 20;
    /**
     * After execute operation.
     *
     * @see com.gigaspaces.internal.client.spaceproxy.ISpaceProxy#execute(SpaceTask,
     * java.lang.Object, Transaction, AsyncFutureListener)
     */
    public final static int AFTER_EXECUTE = 21;
    /**
     * After read operation.
     */
    public final static int AFTER_READ = 22;
    /**
     * After take operation.
     */
    public final static int AFTER_TAKE = 23;
    /**
     * Before change operation.
     */
    public final static int BEFORE_CHANGE = 24;
    /**
     * After change operation.
     */
    public final static int AFTER_CHANGE = 25;

    // the following operation codes can be used ONLY in  non-security
    // filters !!!! (no SpaceContext is available on call to the filter)
    /**
     * On space init. Can be used ONLY in  non-security filters. (no SpaceContext is available when
     * the filter is called)
     *
     * @deprecated this operation code is deprecated. There is no need for ON_INIT , since when
     * filter's init() is called the space is already initialized
     */
    @Deprecated
    public final static int ON_INIT = 51;
    /**
     * Called before entry remove due to lease expression or lease cancel.
     *
     * @see net.jini.core.lease.Lease#cancel()
     */
    public final static int BEFORE_REMOVE = 52;

    /**
     * Called after entry remove due to lease expression or lease cancel.
     *
     * @see net.jini.core.lease.Lease#cancel()
     */
    public final static int AFTER_REMOVE = 53;
    /** */
    public final static int MAX_FILTER_OPERATION_CODES = 55;

    private final static Map<Object, String> fieldsMap = new Hashtable<Object, String>();
    private static int maxCodeValue;

    //logger
    final private static Logger _logger = LoggerFactory.getLogger(Constants.LOGGER_FILTERS);

    static {
        try {
            Field[] fields = FilterOperationCodes.class.getFields();
            for (int i = 0; i < fields.length; i++) {
                Object fieldValue = fields[i].get(null);
                fieldsMap.put(fieldValue, fields[i].getName());
                if (fieldValue instanceof Number)
                    maxCodeValue = StrictMath.max(maxCodeValue, ((Number) fieldValue).intValue());
            }
        } catch (IllegalAccessException ex) {
            if (_logger.isErrorEnabled()) {
                _logger.error(ex.getMessage(), ex);
            }
        }
    }


    /**
     * Don't let anyone instantiate this class.
     */
    private FilterOperationCodes() {
    }

    /**
     * Return a string representation of operation code.
     *
     * @return string representation of the operation code.
     */
    public static String operationCodeToString(Integer code) {
        return fieldsMap.get(code);
    }

    /**
     * Return the number of all filter operation codes.
     *
     * @return the number of all filter operation codes.
     */
    public static int getCount() {
        return fieldsMap.size();
    }

    /**
     * Return the max value of the operation codes.
     *
     * @return the max value of the operation codes.
     */
    public static int getMaxValue() {
        return maxCodeValue;
    }
}
