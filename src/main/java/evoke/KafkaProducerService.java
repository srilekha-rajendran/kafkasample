package evoke;

import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.errors.TopicExistsException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Functioning of this service is absolutely critical to EVokeNet. Logs from this service must be monitored
 * and corrective action taken quickly.
 * Srilekha to implement the methods and delete this line.
 */
class KafkaProducerService extends Thread {


	private final Logger log = LoggerFactory.getLogger(KafkaProducerService.class);

    private String bootstrapAddress;

 
    private String keySerializer;

 
    private String valueSerializer;

    @SuppressWarnings("rawtypes")
    private final KafkaProducer producer;


    private String zookeeperConnect;

    private int sessionTimeoutMs;


    private int connectionTimeoutMs;

   
    private int noOfPartitions;


    private short noOfReplications;

    //private final ZkClient zkClient;
    
   // private final ZkUtils zkUtils;


    boolean isSecureKafkaCluster;


    private boolean isAsync = false;
    
    final int giveUp = 100; // should be configured

    final int max_retry_delay = 30; // should be configured
    
    private  Properties properties;
    
    public KafkaProducerService() {

        /*TODO remove later */

        bootstrapAddress = "172.16.19.143:57457";
        zookeeperConnect =  "localhost:2181";

        sessionTimeoutMs = 10000;
        connectionTimeoutMs = 8000;
        noOfPartitions = 1;
        noOfReplications = 1;
        isSecureKafkaCluster = false;

        System.out.println("In KafkaProducerService .........");
        String serializer = StringSerializer.class.getName();
        String deserializer = StringDeserializer.class.getName();
        
        properties = new Properties();
        properties.put("bootstrap.servers", bootstrapAddress);
       // properties.put("group.id", "test-consumer-group");
        properties.put("key.serializer", "org.apache.kafka.common.serialization.IntegerSerializer");
        properties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        properties.put("enable.auto.commit", "true");    
        properties.put("auto.offset.reset", "earliest");
        properties.put("session.timeout.ms", "30000");
        properties.put("key.deserializer", deserializer);
        properties.put("value.deserializer", deserializer);
        properties.put("key.serializer", serializer);
        properties.put("value.serializer", serializer);
        properties.put("client.id", "1");
        properties.put("acks", "all");
        
     //   properties.put("metadata.broker.list","localhost:57457");
        properties.put("request.required.acks", "1");
        properties.put("broker.id", "0");
      
        // introduce a delay on the send to allow more messages to accumulate
        properties.put(ProducerConfig.LINGER_MS_CONFIG, 1);

        producer = new KafkaProducer(properties);

       // zkClient = new ZkClient(zookeeperConnect, sessionTimeoutMs, connectionTimeoutMs, ZKStringSerializer$.MODULE$);
        
       // zkUtils = new ZkUtils(zkClient, new ZkConnection(zookeeperConnect), false);
        
   
    }

    /**
     * Krishna:
     * Implement this method to post asynchronously so that it does not block. In case of error log the error.
     * @param topic
     * @param message
     */
    @SuppressWarnings({ "unchecked", "rawtypes" })
	int postMessage(String topic, String message) {
    	
    	 try {
    		 String key =  "key";
    		 String value = "value" ;
    		  long startTime = System.currentTimeMillis();
    		  
    		  System.out.println("posting the topic ............");
    		  ProducerRecord<String,String> producerRecord  = new ProducerRecord<String,String>(topic, noOfPartitions,startTime,key,value);    		
    		  
    		  producer.send(producerRecord).get();
    		  producer.close();
			 System.out.println("Sent sucussfully.....................");
			 
		} catch (InterruptedException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		} catch (ExecutionException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
        // log.warn("Sent message: (" + messageNo + ", " + messageStr + ")");
         
      return 1;
    }

    /**
     * Create a charge session topic.
     * @return topic IPAddress:port/topic format
     */
    String createChargeSessionTopic(String topic) {
       

        System.out.println("Creating topic ....." + topic);

       

        try {
        	
        	AdminClient adminClient = AdminClient.create(properties);
        	
        	NewTopic newTopic = new NewTopic(topic,noOfPartitions,noOfReplications);
        	
        	adminClient.createTopics(Collections.singletonList(newTopic));
        	
            System.out.println("created  topic successfully ....." + topic);
        } catch (Exception e) {
            if (!(e.getCause() instanceof TopicExistsException))
                throw new RuntimeException(e.getMessage(), e);
        }
        String address = bootstrapAddress + "/" + topic;
        return address;
    }

    /**
     * Delete a topic. Return 0 on success. Log error at warn level of topic can be deleted. It requires immediate
     * attention if a topic can be deleted.
     * @param topic
     */
    int deleteTopic(String topic) {

        if (topic != null && !topic.isEmpty()) {
            topic = topic.trim();
            try {
                System.out.println("Deleting topic ..." + topic);
                AdminClient adminClient = AdminClient.create(properties);
                adminClient.deleteTopics(Collections.singletonList(topic));
                System.out.println("Deleted sucussfully ..." + topic);
            } catch (Exception ex) {
             
            	System.out.println("Error : Failed to delete topic [" + topic.trim() + "]" + ex.getMessage());
            	ex.printStackTrace();
            }
        }
        return 0;
    }
}

class DemoCallBack implements Callback {
	
	private final Logger log = LoggerFactory.getLogger(DemoCallBack.class);
    private final long startTime;
    private final int key;
    private final String message;

    public DemoCallBack(long startTime, int key, String message) {
        this.startTime = startTime;
        this.key = key;
        this.message = message;
    }
    /**
     * onCompletion method will be called when the record sent to the Kafka Server has been acknowledged.
     *
     * @param metadata The metadata contains the partition and offset of the record. Null if an error occurred.
     * @param exception The exception thrown during processing of this record. Null if no error occurred.
     */
    public void onCompletion(RecordMetadata metadata, Exception exception) {
        long elapsedTime = System.currentTimeMillis() - startTime;
        if (metadata != null) {
            log.error(
                    "message(" + key + ", " + message + ") sent to partition(" + metadata.partition() +
                            "), " +
                            "offset(" + metadata.offset() + ") in " + elapsedTime + " ms");
        } else {
            log.error("Error:  while sending  " + message + " due to " + exception.getMessage());
            exception.printStackTrace();
        }
        /*
         * if(exception != null) {
                          System.out.println('Error while sending  '+message+' due to '+exception.getMessage())
                       } else {
                          System.out.println("The offset of the record we just sent is: " + metadata.offset());
                       }
         */
    }
}
