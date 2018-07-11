package be.kaiaulu;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.LongDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.UUID;
import java.util.stream.Collectors;

public class TestConsumer {

    private static final String BOOTSTRAP_SERVERS = "192.168.4.87:32775";
    private static final String TOPIC = "kafka-example";

    /**
     * Create a consumer for a topic.
     * @return The consumer.
     */
    private static Consumer<Long, String> createConsumer() {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        props.put(ConsumerConfig.CLIENT_ID_CONFIG, "KafkaExampleProducer");
        /*
         * The group id is used to match with an associated offset offset
         */
        props.put(ConsumerConfig.GROUP_ID_CONFIG, UUID.randomUUID().toString());
        /*
         * The offset defines that old messages are to be read also
         */
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, LongDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        return new KafkaConsumer<>(props);
    }

    static void runConsumer() throws InterruptedException {
        final Consumer<Long, String> consumer = createConsumer();

        //consumer.subscribe(Arrays.asList(TOPIC));
        final List<PartitionInfo> partitions = consumer.partitionsFor(TOPIC);
        final List<TopicPartition> partitionList = partitions.stream().map(partition -> new TopicPartition(TOPIC, partition.partition())).collect(Collectors.toList());
        consumer.assign(partitionList);
        while (true) {
            ConsumerRecords<Long, String> records = consumer.poll(100);
            for (ConsumerRecord<Long, String> record : records)
                System.out.printf("offset = %d, key = %s, value = %s%n", record.offset(), record.key(), record.value());
        }
    }

    public static void main(String[] args) throws InterruptedException {
        runConsumer();
    }
}
