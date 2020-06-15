package org.openspaces.persistency.kafka;

import com.gigaspaces.sync.*;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import com.gigaspaces.sync.serializable.EndpointData;

import java.time.Duration;
import java.util.*;
import java.util.stream.Collectors;

public class SpaceSynchronizationEndpointKafkaWriter implements Runnable{
    private static final Log logger = LogFactory.getLog(SpaceSynchronizationEndpointKafkaWriter.class);
    private final static long KAFKA_CONSUME_TIMEOUT = 30;

    private SpaceSynchronizationEndpoint spaceSynchronizationEndpoint;
    private Consumer<String, EndpointData> kafkaConsumer;
    private final String topic;

    public SpaceSynchronizationEndpointKafkaWriter(SpaceSynchronizationEndpoint spaceSynchronizationEndpoint, Properties kafkaProps, String topic) {
        this.kafkaConsumer = new KafkaConsumer<>(kafkaProps);
        this.spaceSynchronizationEndpoint = spaceSynchronizationEndpoint;
        this.topic = topic;
    }

    public void run() {
        Set<TopicPartition> topicPartitions = getTopicPartitions();
        kafkaConsumer.assign(topicPartitions);
        try{
            while (true) {
                try {
                    Map<TopicPartition, OffsetAndMetadata> map = kafkaConsumer.committed(topicPartitions);
                    Collection<TopicPartition> uninitializedPartitions = new HashSet<>();
                    for (Map.Entry<TopicPartition, OffsetAndMetadata> entry : map.entrySet()) {
                        TopicPartition topicPartition = entry.getKey();
                        OffsetAndMetadata offsetAndMetadata = entry.getValue();
                        if(offsetAndMetadata != null) {
                            kafkaConsumer.seek(topicPartition, offsetAndMetadata);
                        }
                        else{
                            uninitializedPartitions.add(topicPartition);
                        }
                    }
                    if(!uninitializedPartitions.isEmpty())
                        kafkaConsumer.seekToBeginning(uninitializedPartitions);
                    ConsumerRecords<String, EndpointData> records = kafkaConsumer.poll(Duration.ofSeconds(KAFKA_CONSUME_TIMEOUT));
                    for(ConsumerRecord<String, EndpointData> record: records) {
                        EndpointData endpointData = record.value();
                        switch (endpointData.getSyncEndpointMethod()) {
                            case onTransactionConsolidationFailure:
                                spaceSynchronizationEndpoint.onTransactionConsolidationFailure((ConsolidationParticipantData) endpointData);
                                logRecord(ConsolidationParticipantData.class.getSimpleName());
                                kafkaConsumer.commitSync();
                                break;
                            case onTransactionSynchronization:
                                spaceSynchronizationEndpoint.onTransactionSynchronization((TransactionData) endpointData);
                                try{
                                    spaceSynchronizationEndpoint.afterTransactionSynchronization((TransactionData) endpointData);
                                }catch (Exception e){
                                    if(logger.isWarnEnabled()) {
                                        logger.warn("Caught exception while attempting afterTransactionSynchronization: " + e.getMessage());
                                    }
                                }
                                logRecord(TransactionData.class.getSimpleName());
                                kafkaConsumer.commitSync();
                                break;
                            case onOperationsBatchSynchronization:
                                spaceSynchronizationEndpoint.onOperationsBatchSynchronization((OperationsBatchData) endpointData);
                                try{
                                    spaceSynchronizationEndpoint.afterOperationsBatchSynchronization((OperationsBatchData) endpointData);
                                }catch (Exception e){
                                    if(logger.isWarnEnabled()) {
                                        logger.warn("Caught exception while attempting afterOperationsBatchSynchronization: " + e.getMessage());
                                    }
                                }
                                logRecord(OperationsBatchData.class.getSimpleName());
                                kafkaConsumer.commitSync();
                                break;
                            case onAddIndex:
                                spaceSynchronizationEndpoint.onAddIndex((AddIndexData) endpointData);
                                logRecord(AddIndexData.class.getSimpleName());
                                kafkaConsumer.commitSync();
                                break;
                            case onIntroduceType:
                                spaceSynchronizationEndpoint.onIntroduceType((IntroduceTypeData) endpointData);
                                logRecord(IntroduceTypeData.class.getSimpleName());
                                kafkaConsumer.commitSync();
                                break;
                        }
                    }
                }catch (Exception e){
                    logException(e);
                    continue;
                }
            }
        } finally {
            if(logger.isInfoEnabled())
                logger.info("Closing kafka consumer of topic " + topic);
            kafkaConsumer.close();
        }
    }

    private Set<TopicPartition> getTopicPartitions(){
        List<PartitionInfo> partitionInfos;
        while (true){
            try{
                partitionInfos = kafkaConsumer.partitionsFor(topic);
                if(partitionInfos != null)
                    return partitionInfos.stream().map(p -> new TopicPartition(p.topic(), p.partition())).collect(Collectors.toSet());
            } catch (RuntimeException e){
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException interruptedException) {
                    throw new RuntimeException("Iterrupted while getting kafka topic partitions for topic " + topic);
                }
            }
        }
    }

    private void logException(Exception e) {
        if(logger.isWarnEnabled())
            logger.warn("Caught exception while consuming Kafka records: " + e.getMessage());
    }

    private void logRecord(String endpointType){
        if(logger.isDebugEnabled()) {
            logger.debug("Consumed kafka message of type " + endpointType + " and persisted to " + spaceSynchronizationEndpoint.getClass().getSimpleName());
        }
    }

    public void close() {
        kafkaConsumer.close();
    }
}
