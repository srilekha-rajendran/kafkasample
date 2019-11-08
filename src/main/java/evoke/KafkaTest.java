package evoke;

public class KafkaTest {

	public static void main(String[] args) {
		// TODO Auto-generated method stub

		KafkaProducerService producer = new KafkaProducerService();
		String topic = "TopicChargeSession";
		String message = "Message ....";
		//Topic1,testTopic,TOPICSRILEKHA7
		
		producer.deleteTopic("TopicChargeSession");
		
		producer.createChargeSessionTopic(topic);
		producer.postMessage(topic, message);
		
		//KafkaConsumerService consumer = new KafkaConsumerService();
		
		//consumer.subcribeTopic(topic);
	}

}
