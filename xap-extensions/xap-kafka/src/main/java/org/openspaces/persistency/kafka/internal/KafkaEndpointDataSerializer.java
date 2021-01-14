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
package org.openspaces.persistency.kafka.internal;

import com.gigaspaces.api.InternalApi;
import com.gigaspaces.sync.serializable.EndpointData;
import com.j_spaces.kernel.CommonsLangUtils;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.serialization.Serializer;

import java.util.Map;

@InternalApi
public class KafkaEndpointDataSerializer implements Serializer<EndpointData> {
    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {

    }

    @Override
    public byte[] serialize(String topic, EndpointData data) {
        return CommonsLangUtils.serialize(data);
    }

    @Override
    public byte[] serialize(String topic, Headers headers, EndpointData data) {
        return CommonsLangUtils.serialize(data);
    }

    @Override
    public void close() {

    }
}
