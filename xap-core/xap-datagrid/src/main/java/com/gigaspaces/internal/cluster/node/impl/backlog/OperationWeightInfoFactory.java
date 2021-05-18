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
package com.gigaspaces.internal.cluster.node.impl.backlog;

/**
 * @author yael nahon
 * @since 12.1
 */
public class OperationWeightInfoFactory {

    private static final OperationWeightInfo SINGLE_WRITE = new OperationWeightInfo(1, WeightInfoOperationType.WRITE);
    private static final OperationWeightInfo SINGLE_UPDATE = new OperationWeightInfo(1, WeightInfoOperationType.UPDATE);
    private static final OperationWeightInfo SINGLE_TAKE = new OperationWeightInfo(1, WeightInfoOperationType.TAKE);
    private static final OperationWeightInfo SINGLE_CHANGE = new OperationWeightInfo(1, WeightInfoOperationType.CHANGE);
    private static final OperationWeightInfo SINGLE_NOTIFY = new OperationWeightInfo(1, WeightInfoOperationType.NOTIFY);
    private static final OperationWeightInfo SINGLE_PREPARE = new OperationWeightInfo(1, WeightInfoOperationType.PREPARE);

    private static final OperationWeightInfo UNDEFINED_WRITE = new OperationWeightInfo(WeightInfoOperationType.WRITE);
    private static final OperationWeightInfo UNDEFINED_UPDATE = new OperationWeightInfo(WeightInfoOperationType.UPDATE);
    private static final OperationWeightInfo UNDEFINED_TAKE = new OperationWeightInfo(WeightInfoOperationType.TAKE);
    private static final OperationWeightInfo UNDEFINED_CHANGE = new OperationWeightInfo(WeightInfoOperationType.CHANGE);
    private static final OperationWeightInfo UNDEFINED_NOTIFY = new OperationWeightInfo(WeightInfoOperationType.NOTIFY);
    private static final OperationWeightInfo UNDEFINED_PREPARE = new OperationWeightInfo(WeightInfoOperationType.PREPARE);

    public static OperationWeightInfo create(int numOfOperations, WeightInfoOperationType type){
        if(numOfOperations != 1){
            return new OperationWeightInfo(numOfOperations, type);
        }

        switch (type){
            case WRITE: return SINGLE_WRITE;
            case UPDATE: return SINGLE_UPDATE;
            case TAKE: return SINGLE_TAKE;
            case CHANGE: return SINGLE_CHANGE;
            case NOTIFY: return SINGLE_NOTIFY;
            case PREPARE: return SINGLE_PREPARE;
            default: throw new UnsupportedOperationException("No such operation type: " + type);
        }
    }

    public static OperationWeightInfo create(WeightInfoOperationType type){
        switch (type){
            case WRITE: return UNDEFINED_WRITE;
            case UPDATE: return UNDEFINED_UPDATE;
            case TAKE: return UNDEFINED_TAKE;
            case CHANGE: return UNDEFINED_CHANGE;
            case NOTIFY: return UNDEFINED_NOTIFY;
            case PREPARE: return UNDEFINED_PREPARE;
            default: throw new UnsupportedOperationException("No such operation type: " + type);
        }
    }
}
