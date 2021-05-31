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


package com.j_spaces.core;

/**
 * A Constants class for status of a transaction in the transactions table.
 */
public enum XtnStatus {
    // initial transaction state - before calling join
    UNINITIALIZED,
    // uninitialized and join failed- xtn is removed
    UNINITIALIZED_FAILED,

    // transaction state after successful join
    BEGUN,
    PREPARING,
    PREPARED,
    COMMITING,
    COMMITED,
    ROLLING,
    ROLLED,
    //error occured- will try to rollback
    ERROR,
    //xtn is marked as unused, will be cleaned by LeaseManager
    UNUSED;

    public boolean isFinalizing() {
        switch (this) {
            case PREPARING:
            case PREPARED:
            case COMMITING:
            case COMMITED:
            case ROLLING:
            case ROLLED:
                return true;
            default:
                return false;
        }
    }

}

