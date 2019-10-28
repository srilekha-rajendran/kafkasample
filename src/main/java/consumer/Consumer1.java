package consumer;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

public class Consumer1 {
	
	 public static final String KAFKA_SERVER_URL = "localhost";
	    public static final int KAFKA_SERVER_PORT = 9092;
	    public static final String CLIENT_ID = "SampleProducer";
	    
	    private final static String TOPIC = "my-topic1";
	    private final static String BOOTSTRAP_SERVERS =
	            "localhost:9092,localhost:9093,localhost:9094";

	    public static void main(String args[]) {
	    	
	    	Properties props = new Properties();
		     
	 	    props.put("bootstrap.servers", BOOTSTRAP_SERVERS);
	 	    
	 	    
	 	     props.put("group.id", "test");
	 	     props.put("enable.auto.commit", "false");
	 	     props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
	 	     props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
	 	     
	 	     
	 	     KafkaConsumer consumer = new KafkaConsumer(props);
	 	     
	 	    // consumer.subscribe(Arrays.asList("my-topic1", "my-topic2"));
	 	     
	 	    consumer.subscribe(Collections.singletonList(TOPIC));
	 	    
	 	     final int minBatchSize = 200;
	 	     List<ConsumerRecord<String, String>> buffer = new ArrayList<ConsumerRecord<String, String>>();
	 	     while (true) {
	 	         ConsumerRecords<String, String> records = consumer.poll(100);
	 	         for (ConsumerRecord<String, String> record : records) {
	 	             buffer.add(record);
	 	         }
	 	         if (buffer.size() >= minBatchSize) {
	 	             //insertIntoDb(buffer);
	 	             //consumer.commitSync();
	 	             buffer.clear();
	 	         }
	 	     }
	    }
	   
}