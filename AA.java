import com.sun.jna.*;
import java.util.Properties;
import java.util.Arrays;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.ConsumerRecord;

/*public class Client {
  public interface Cosi extends Library {
    public long Add(long a, long b);
  }
  static public void main(String argv[]) {
    Cosi cosi = (Cosi) Native.loadLibrary(
      "./cosi.so", Cosi.class);
    System.out.printf(cosi.Add(12, 99));
  }
}*/
public class AA {
	
	public static void main(String [] args) throws Exception {
		List<String> content=pollContent();
		String collectiveSig=collectiveSigning(content);
		sendCollectiveSigToCC(collectiveSig);
    }
    private String collectiveSigning(List<String> content) {
    	return "todo";
    }
    private List<String> pollContent() {
        String topicName = "CC_TO_AA";
		Properties props = new Properties();

		props.put("bootstrap.servers", "localhost:9092");
		props.put("group.id", "test");
		props.put("enable.auto.commit", "true");
		props.put("auto.commit.interval.ms", "1000");
		props.put("session.timeout.ms", "30000");
		props.put("key.deserializer", 
		 "org.apache.kafka.common.serializa-tion.StringDeserializer");
		props.put("value.deserializer", 
		 "org.apache.kafka.common.serializa-tion.StringDeserializer");

		KafkaConsumer<String, List<String>> consumer = new KafkaConsumer<String, List<String>>(props);
		consumer.subscribe(Arrays.asList(topicName))

		System.out.println("AA Subscribed to topic " + topicName);
		//have to poll infinitely, otherwise detected as dead
		while (true) {
			ConsumerRecords<String, List<String>> records = consumer.poll(500);//timeout in milisecond
			for (ConsumerRecord<String, List<String>> record : records)
				return record.value();
		}
    }

    private void sendCollectiveSigToCC(String collectiveSig) {
    	//Assign topicName to string variable
		String topicName = "AA_TO_CC"; 
		Properties props = new Properties();

		//Assign localhost id
		props.put("bootstrap.servers", "localhost:9092");  
		props.put("acks", "all");
		props.put("retries", 0);
		props.put("batch.size", 16384);
		props.put("buffer.memory", 33554432);
		props.put("key.serializer", 
		 "org.apache.kafka.common.serializa-tion.StringSerializer");
		props.put("value.serializer", 
		 "org.apache.kafka.common.serializa-tion.StringSerializer");
        //send to kafka
    	Producer<String, String> producer = new KafkaProducer<String, String>(props);
	   
		producer.send(new ProducerRecord<String, String>(topicName,"1", collectiveSig));//key,value
		System.out.println("Collective Signature sent to CC successfully");
		producer.close();
    }
}