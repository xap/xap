package org.openspaces.persistency.kafka.internal;

import com.gigaspaces.api.InternalApi;
import com.gigaspaces.sync.serializable.EndpointData;
import org.apache.commons.lang.SerializationUtils;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.serialization.Deserializer;

import java.util.Map;

@InternalApi
public class KafkaEndpointDataDeserializer implements Deserializer<EndpointData> {
    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {

    }

    @Override
    public EndpointData deserialize(String topic, byte[] data) {
        return (EndpointData) SerializationUtils.deserialize(data);
    }

    @Override
    public EndpointData deserialize(String topic, Headers headers, byte[] data) {
        return (EndpointData) SerializationUtils.deserialize(data);
    }
}
