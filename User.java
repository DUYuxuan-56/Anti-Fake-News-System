import org.slf4j.LoggerFactory;
import java.util.Properties;
import java.util.*;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.ConsumerRecord;

public class User {
	
	public static void main(String [] args) throws Exception {
		String content = pollContent();
		String hash = hash(content);
		String sig = query(hash);
		if (sig!=null){
			//cosi.Verify(testSuite, publics, message, sig, p)
		};

    }
    private static String hash(String content) {
    	//String sha256hex = org.apache.commons.codec.digest.DigestUtils.sha256Hex(content); 
    	return "todo";
    }
    private static String pollContent() {
        String topicName = "CC_TO_User";
		Properties props = new Properties();

		props.put("bootstrap.servers", "manta.uwaterloo.ca:9092");
		props.put("group.id", "test");
		props.put("enable.auto.commit", "true");
		props.put("auto.commit.interval.ms", "1000");
		props.put("session.timeout.ms", "30000");
		props.put("key.deserializer", 
		 "org.apache.kafka.common.serialization.StringDeserializer");
		props.put("value.deserializer", 
		 "org.apache.kafka.common.serialization.StringDeserializer");

		KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(props);
		consumer.subscribe(Arrays.asList(topicName));

		System.out.println("USER Subscribed to topic " + topicName);
		//have to poll infinitely, otherwise detected as dead
		while (true) {
			ConsumerRecords<String, String> records = consumer.poll(500);//timeout in milisecond
			for (ConsumerRecord<String, String> record : records)
				return record.value();
		}
    }
    private static String query(String sig) {
    	return "";
    }
}