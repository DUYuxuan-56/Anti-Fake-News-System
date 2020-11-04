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

public class CC {
   private static List<String> l;

   public static void main(String[] args) throws Exception{

		genContent();
		broadcastContentToAA();
		//broadcastContentToUser();
		//wait for a while
		String sig=pollCollectiveSig();
		String hash=hash(l);
		commit(sig,hash);
	}
	private static String hash(List<String> content) {
		return "todo";
    }
	private static void genContent() {
        l = new ArrayList<String>(1024);
        String somebigContent = "faldskfjalkdsjfalkfdjasfoeiurqoeueoirqueroqiewurewqodfnmdsalkfjdsalkfjaslkfajflasdjfadslfkajdflkjfdalkadfjlkdfjfadsflkjafaldskfjalkdsjfalkfdjasfoeiurqoeueoirqueroqiewurewqodfnmdsalkfjdsalkfjaslkfajflasdjfadslfkajdflkjfdalkadfjlkdfjfadsflkjafaldskfjalkdsjfalkfdjasfoeiurqoeueoirqueroqiewurewqodfnmdsalkfjdsalkfjaslkfajflasdjfadslfkajdflkjfdalkadfjlkdfjfadsflkjafaldskfjalkdsjfalkfdjasfoeiurqoeueoirqueroqiewurewqodfnmdsalkfjdsalkfjaslkfajflasdjfadslfkajdflkjfdalkadfjlkdfjfadsflkjafaldskfjalkdsjfalkfdjasfoeiurqoeueoirqueroqiewurewqodfnmdsalkfjdsalkfjaslkfajflasdjfadslfkajdflkjfdalkadfjlkdfjfadsflkjafaldskfjalkdsjfalkfdjasfoeiurqoeueoirqueroqiewurewqodfnmdsalkfjdsalkfjaslkfajflasdjfadslfkajdflkjfdalkadfjlkdfjfadsflkjafaldskfjalkdsjfalkfdjasfoeiurqoeueoirqueroqiewurewqodfnmdsalkfjdsalkfjaslkfajflasdjfadslfkajdflkjfdalkadfjlkdfjfadsflkjafaldskfjalkdsjfalkfdjasfoeiurqoeueoirqueroqiewurewqodfnmdsalkfjdsalkfjaslkfajflasdjfadslfkajdflkjfdalkadfjlkdfjfadsflkjafaldskfjalkdsjfalkfdjasfoeiurqoeueoirqueroqiewurvcvmvcmdoiZZ";
        for (int i = 0; i < 10; i++) {
            l.add(somebigContent + i);
        }
    }
    private static void broadcastContentToUser() {
		String topicName = "CC_TO_User"; 
		Properties props = new Properties();

		props.put("bootstrap.servers", "manta.uwaterloo.ca:9092");  
		props.put("acks", "all");
		props.put("retries", 0);
		props.put("batch.size", 16384);
		props.put("buffer.memory", 33554432);
		props.put("key.serializer", 
		 "org.apache.kafka.common.serialization.StringSerializer");
		props.put("value.serializer", 
		 "org.apache.kafka.common.serialization.StringSerializer");
        //send to kafka
    	Producer<String, String> producer = new KafkaProducer<String, String>(props);
	   
	    String string = String.join(",", l);
		producer.send(new ProducerRecord<String, String>(topicName,"1", string));//key,value
		System.out.println("Content sent to user successfully");
		producer.close();
    }
    private static void broadcastContentToAA() {
    	//Assign topicName to string variable
		String topicName = "CC_TO_AA"; 
		Properties props = new Properties();
		props.put("bootstrap.servers", "manta.uwaterloo.ca:9092");  
		props.put("acks", "all");
		props.put("retries", 0);
		props.put("batch.size", 16384);
		props.put("buffer.memory", 33554432);
		props.put("key.serializer", 
		 "org.apache.kafka.common.serialization.StringSerializer");
		props.put("value.serializer", 
		 "org.apache.kafka.common.serialization.StringSerializer");
        //send to kafka
    	Producer<String, String> producer = new KafkaProducer<String, String>(props);
	   
	    String string = String.join(",", l);
		producer.send(new ProducerRecord<String, String>(topicName,"1", string));//key,value
		System.out.println("Content sent to AA successfully");
		producer.close();
    }
    private static String pollCollectiveSig() {
        String topicName = "AA_TO_CC";
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

		System.out.println("CC Subscribed to topic to get cosi" + topicName);
		//have to poll infinitely, otherwise detected as dead
		while (true) {
			ConsumerRecords<String, String> records = consumer.poll(500);//timeout in milisecond
			for (ConsumerRecord<String, String> record : records)
				return record.value();
		}
    }
    private static void commit(String sig,String hash) {
    	//signature + hash of the content
    	return;
    }
}	