import java.util.Properties;
import java.util.Arrays;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.ConsumerRecord;

public class User {
	static Logger log;
	
	public static void main(String [] args) throws Exception {
		List<String> content=pollContent();
		String hash=hash(content);
		query(hash);

    }
    private String hash(List<String> content) {
    	return "todo";
    }
    private List<String> pollContent() {
        String topicName = "CC_TO_User";
		Properties props = new Properties();
		List<String> res = new ArrayList<String>(1024);

		props.put("bootstrap.servers", "localhost:9092");
		props.put("group.id", "test");
		props.put("enable.auto.commit", "true");
		props.put("auto.commit.interval.ms", "1000");
		props.put("session.timeout.ms", "30000");
		props.put("key.deserializer", 
		 "org.apache.kafka.common.serializa-tion.StringDeserializer");
		props.put("value.deserializer", 
		 "org.apache.kafka.common.serializa-tion.StringDeserializer");

		KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(props);
		consumer.subscribe(Arrays.asList(topicName))

		System.out.println("USER Subscribed to topic " + topicName);
		//have to poll infinitely, otherwise detected as dead
		while (true) {
			ConsumerRecords<String, String> records = consumer.poll(500);//timeout in milisecond
			for (ConsumerRecord<String, String> record : records)
				res.add(record.value());
			return res;
		}
    }
    private void query(String sig) {
    	return;
    }
}