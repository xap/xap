package org.openspaces.persistency.kafka;

import com.gigaspaces.sync.*;
import com.gigaspaces.sync.serializable.EndpointData;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;

import java.time.Duration;
import java.util.*;
import java.util.stream.Collectors;

import static org.openspaces.persistency.kafka.KafkaSpaceSynchronizationEndpoint.KAFKA_TIMEOUT;

public class SpaceSynchronizationEndpointKafkaWriter implements Runnable{
    public static final Duration TIMEOUT = Duration.ofSeconds(KAFKA_TIMEOUT);

    private final Log logger;
    private final SpaceSynchronizationEndpoint spaceSynchronizationEndpoint;
    private final Consumer<String, EndpointData> kafkaConsumer;
    private final String topic;
    private final Set<TopicPartition> topicPartitions;
    private Map<TopicPartition, OffsetAndMetadata> startingPoint;
    private boolean firstTime = true;

    public SpaceSynchronizationEndpointKafkaWriter(SpaceSynchronizationEndpoint spaceSynchronizationEndpoint, Properties kafkaProps, String topic , String groupName) {
        this.logger = LogFactory.getLog(this.getClass().getName() + "." + groupName);
        this.spaceSynchronizationEndpoint = spaceSynchronizationEndpoint;
        this.topic = topic;
        kafkaProps.put(ConsumerConfig.GROUP_ID_CONFIG, groupName);
        this.kafkaConsumer = new KafkaConsumer<>(kafkaProps);
        this.topicPartitions = initTopicPartitions();
        kafkaConsumer.assign(topicPartitions);
        this.startingPoint = kafkaConsumer.committed(topicPartitions);
        if(startingPoint.values().stream().allMatch(Objects::isNull))
            startingPoint = null;
    }

    private Set<TopicPartition> initTopicPartitions(){
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
                    throw new RuntimeException("Interrupted while getting kafka partitions for topic " + topic);
                }
            }
        }
    }

    public Map<TopicPartition, OffsetAndMetadata> getStartingPoint() {
        return startingPoint;
    }

    public void setStartingPoint(Map<TopicPartition, OffsetAndMetadata> startingPoint) {
        this.startingPoint = startingPoint;
    }

    public void run() {
        while (true) {
            try {
                beforePoll();
                ConsumerRecords<String, EndpointData> records = kafkaConsumer.poll(TIMEOUT);
                processRecords(records);
            }
            catch (Exception e){
                if(isInterrupted(e)){
                    handleInterrupted();
                    break;
                }
                if(logger.isWarnEnabled())
                    logger.warn("Caught exception while consuming Kafka records" ,e);
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException interruptedException) {
                    handleInterrupted();
                    break;
                }
            }
        }
    }
    
    private boolean isInterrupted(Throwable e) {
        if (e instanceof InterruptedException)
            return true;
        Throwable cause = e.getCause();
        return cause != null && isInterrupted(cause);
    }
    
    private void handleInterrupted(){
        if(logger.isInfoEnabled())
            logger.info("Closing kafka consumer of topic " + topic);
        kafkaConsumer.close(TIMEOUT);
    }
    
    private void beforePoll() {
        Map<TopicPartition, OffsetAndMetadata> map;
        if(firstTime){
            map = startingPoint;
            firstTime = false;
        }
        else {
            map = kafkaConsumer.committed(topicPartitions);
        }
        if(map != null) {
            Collection<TopicPartition> uninitializedPartitions = new HashSet<>();
            for (Map.Entry<TopicPartition, OffsetAndMetadata> entry : map.entrySet()) {
                TopicPartition topicPartition = entry.getKey();
                OffsetAndMetadata offsetAndMetadata = entry.getValue();
                if (offsetAndMetadata != null) {
                    kafkaConsumer.seek(topicPartition, offsetAndMetadata);
                } else {
                    uninitializedPartitions.add(topicPartition);
                }
            }
            if (!uninitializedPartitions.isEmpty())
                kafkaConsumer.seekToBeginning(uninitializedPartitions);
        }
    }

    private void processRecords(ConsumerRecords<String, EndpointData> records) throws InterruptedException{
        for(ConsumerRecord<String, EndpointData> record: records) {
            EndpointData endpointData = record.value();
            switch (endpointData.getSyncEndpointMethod()) {
                case onTransactionConsolidationFailure:
                    spaceSynchronizationEndpoint.onTransactionConsolidationFailure((ConsolidationParticipantData) endpointData);
                    logRecord(ConsolidationParticipantData.class.getSimpleName());
                    commit();
                    break;
                case onTransactionSynchronization:
                    spaceSynchronizationEndpoint.onTransactionSynchronization((TransactionData) endpointData);
                    try {
                        spaceSynchronizationEndpoint.afterTransactionSynchronization((TransactionData) endpointData);
                    } catch (Exception e) {
                        if (logger.isWarnEnabled()) {
                            logger.warn("Caught exception while attempting afterTransactionSynchronization: " + e.getMessage());
                        }
                    }
                    logRecord(TransactionData.class.getSimpleName());
                    commit();
                    break;
                case onOperationsBatchSynchronization:
                    spaceSynchronizationEndpoint.onOperationsBatchSynchronization((OperationsBatchData) endpointData);
                    try {
                        spaceSynchronizationEndpoint.afterOperationsBatchSynchronization((OperationsBatchData) endpointData);
                    } catch (Exception e) {
                        if (logger.isWarnEnabled()) {
                            logger.warn("Caught exception while attempting afterOperationsBatchSynchronization: " + e.getMessage());
                        }
                    }
                    logRecord(OperationsBatchData.class.getSimpleName());
                    commit();
                    break;
                case onAddIndex:
                    spaceSynchronizationEndpoint.onAddIndex((AddIndexData) endpointData);
                    logRecord(AddIndexData.class.getSimpleName());
                    commit();
                    break;
                case onIntroduceType:
                    spaceSynchronizationEndpoint.onIntroduceType((IntroduceTypeData) endpointData);
                    logRecord(IntroduceTypeData.class.getSimpleName());
                    commit();
                    break;
            }
        }
    }
    
    private void commit() throws InterruptedException{
        while (true) {
            try {
                kafkaConsumer.commitSync();
                return;
            } catch (Exception e) {
                if(logger.isWarnEnabled())
                    logger.warn("Caught exception while committing Kafka message consumption: " + e.getMessage());
                Thread.sleep(1000);
            }
        }
    }
    
    private void logRecord(String endpointType){
        if(logger.isDebugEnabled()) {
            logger.debug("Consumed kafka message of type " + endpointType + " and persisted to " + spaceSynchronizationEndpoint.getClass().getSimpleName());
        }
    }
}