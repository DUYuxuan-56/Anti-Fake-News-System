//import com.sun.jna.*;
//import org.apache.kafka.common.serialization.Serdes;
import org.slf4j.LoggerFactory;
import java.util.Properties;
import java.util.*;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.ConsumerRecord;

public class AA {
	
	public static void main(String [] args) throws Exception {
		String content=pollContent();
		String collectiveSig=collectiveSigning(content);
		sendCollectiveSigToCC(collectiveSig);
    }
    private static String collectiveSigning(String content) {
    	return "todo";
    }
    private static String pollContent() throws Exception{
        String topicName = "CC_TO_AA";
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

		System.out.println("AA Subscribed to topic " + topicName);
		//have to poll infinitely, otherwise detected as dead
		while (true) {
			ConsumerRecords<String, String> records = consumer.poll(500);//timeout in milisecond
			for (ConsumerRecord<String, String> record : records)
				return record.value();
		}
    }

    private static void sendCollectiveSigToCC(String collectiveSig) throws Exception{
    	//Assign topicName to string variable
		String topicName = "AA_TO_CC"; 
		Properties props = new Properties();

		//Assign localhost id
		props.put("bootstrap.servers", "manta.uwaterloo.ca:9092");  
		props.put("acks", "all");
		props.put("retries", 0);
		props.put("batch.size", 16384);
		props.put("buffer.memory", 33554432);
		props.put("key.serializer", 
		 Class.forName("org.apache.kafka.common.serialization.StringSerializer"));
		props.put("value.serializer", 
		 Class.forName("org.apache.kafka.common.serialization.StringSerializer"));
        //send to kafka
    	Producer<String, String> producer = new KafkaProducer<String, String>(props);
	   
		producer.send(new ProducerRecord<String, String>(topicName,"1", collectiveSig));//key,value
		System.out.println("Collective Signature sent to CC successfully");
		producer.close();
    }
}