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
package com.gigaspaces.proxy_pool;


import java.util.HashMap;

public class HashValue {
//    HashMapBusy<SpaceName, ref counter + GConnection>
//            , HashMapIdle<SpaceName, timestamp>

    final private HashMap<String, BusyValue> hashMapBusy;
    final private HashMap<String, Long> hashMapIdle;


    public HashValue() {
        hashMapBusy = new HashMap<>();
        hashMapIdle = new HashMap<>();
    }

    @Override
    public String toString() {
        return "HashValue{" +
                "hashMapBusy=" + hashMapBusy +
                ", hashMapIdle=" + hashMapIdle +
                '}';
    }

    public HashMap<String, BusyValue> getHashMapBusy() {
        return hashMapBusy;
    }

    public HashMap<String, Long> getHashMapIdle() {
        return hashMapIdle;
    }

    public boolean isEmpty (){
        return hashMapBusy.size() == 0 && hashMapIdle.size() == 0;
    }

}
