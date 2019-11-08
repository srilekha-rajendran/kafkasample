package evoke;


import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class KafkaConsumerService {


	private final Logger log = LoggerFactory.getLogger(KafkaConsumerService.class);
	
    private String bootstrapAddress;

    private KafkaConsumer consumer;


    final Duration timeout = Duration.ofSeconds(30);

    final int giveUp = 100;

    public KafkaConsumerService() {

        Properties props = new Properties();
        
        bootstrapAddress = "localhost:9092";
        
        props.put("bootstrap.servers", bootstrapAddress);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "test-consumer-group");
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");
        props.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "1000");
        props.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, "30000");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.IntegerDeserializer");
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");

        consumer = new KafkaConsumer(props);
    }

    void subcribeTopic(String topic) {
        consumer.subscribe(Arrays.asList(topic));
        for (int i = 0; i < giveUp; i++) {
            ConsumerRecords<String, String> records = consumer.poll(100);
            log.trace("Size: " + records.count());
            System.out.println("Size: " + records.count());
            for (ConsumerRecord<String, String> record : records) {
            	System.out.println("Received a message: " + record.key() + " " + record.value());
            }
        }
        System.out.println("----------   End  ----------");
    }
}
