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

package com.gigaspaces.internal.server.space.operations;

/**
 * @author Niv Ingberg
 * @since 9.0.0
 */
@com.gigaspaces.api.InternalApi
public class SpaceOperationsCodes {
    public static final int NUM_OF_OPERATIONS = 23;

    public static final int EXECUTE_TASK = 0;
    public static final int ABORT_TRANSACTION = 1;
    public static final int COMMIT_TRANSACTION = 2;
    public static final int PREPARE_AND_COMMIT_TRANSACTION = 3;
    public static final int UPDATE_LEASE = 4;
    public static final int UPDATE_LEASES = 5;
    public static final int GET_ENTRY_TYPE_DESCRIPTOR = 6;
    public static final int REGISTER_ENTRY_TYPE_DESCRIPTOR = 7;
    public static final int ADD_ENTRY_TYPE_INDEXES = 8;
    public static final int WRITE_ENTRY = 9;
    public static final int WRITE_ENTRIES = 10;
    public static final int READ_TAKE_ENTRY = 11;
    public static final int READ_TAKE_ENTRIES = 12;
    public static final int READ_TAKE_ENTRIES_BY_IDS = 13;
    public static final int READ_TAKE_ENTRIES_UIDS = 14;
    public static final int COUNT_CLEAR_ENTRIES = 15;
    public static final int REGISTER_ENTRIES_LISTENER = 16;
    // Since 9.1
    public static final int CHANGE_ENTRIES = 17;
    // Since 9.5
    public static final int REGISTER_LOCAL_VIEW = 18;
    public static final int UNREGISTER_LOCAL_VIEW = 19;
    // Since 10.0
    public static final int AGGREGATE_ENTRIES = 20;

    private SpaceOperationsCodes() {
    }
}
