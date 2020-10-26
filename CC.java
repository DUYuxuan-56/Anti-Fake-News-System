import java.util.Properties;
import java.util.Arrays;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.ConsumerRecord;

public class CC {
   private List<String> l;

   public static void main(String[] args) throws Exception{

		genContent();
		broadcastContentToAA();
		//broadcastContentToUser();
		//wait for a while
		String sig=pollCollectiveSig();
		String hash=hash(l);
		commit(sig,hash);
	}
	private String hash(List<String> content) {
		return "todo";
    }
	private void genContent() {
        l = new ArrayList<String>(1024);
        String somebigContent = "faldskfjalkdsjfalkfdjasfoeiurqoeueoirqueroqiewurewqodfnmdsalkfjdsalkfjaslkfajflasdjfadslfkajdflkjfdalkadfjlkdfjfadsflkjafaldskfjalkdsjfalkfdjasfoeiurqoeueoirqueroqiewurewqodfnmdsalkfjdsalkfjaslkfajflasdjfadslfkajdflkjfdalkadfjlkdfjfadsflkjafaldskfjalkdsjfalkfdjasfoeiurqoeueoirqueroqiewurewqodfnmdsalkfjdsalkfjaslkfajflasdjfadslfkajdflkjfdalkadfjlkdfjfadsflkjafaldskfjalkdsjfalkfdjasfoeiurqoeueoirqueroqiewurewqodfnmdsalkfjdsalkfjaslkfajflasdjfadslfkajdflkjfdalkadfjlkdfjfadsflkjafaldskfjalkdsjfalkfdjasfoeiurqoeueoirqueroqiewurewqodfnmdsalkfjdsalkfjaslkfajflasdjfadslfkajdflkjfdalkadfjlkdfjfadsflkjafaldskfjalkdsjfalkfdjasfoeiurqoeueoirqueroqiewurewqodfnmdsalkfjdsalkfjaslkfajflasdjfadslfkajdflkjfdalkadfjlkdfjfadsflkjafaldskfjalkdsjfalkfdjasfoeiurqoeueoirqueroqiewurewqodfnmdsalkfjdsalkfjaslkfajflasdjfadslfkajdflkjfdalkadfjlkdfjfadsflkjafaldskfjalkdsjfalkfdjasfoeiurqoeueoirqueroqiewurewqodfnmdsalkfjdsalkfjaslkfajflasdjfadslfkajdflkjfdalkadfjlkdfjfadsflkjafaldskfjalkdsjfalkfdjasfoeiurqoeueoirqueroqiewurvcvmvcmdoiZZ";
        for (int i = 0; i < 10; i++) {
            l.add(somebigContent + i);
        }
    }
    private void broadcastContentToUser() {
		String topicName = "CC_TO_User"; 
		Properties props = new Properties();

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
    	Producer<String, List<String>> producer = new KafkaProducer<String, List<String>>(props);
	   
		producer.send(new ProducerRecord<String, List<String>>(topicName,"1", l));//key,value
		System.out.println("Content sent to user successfully");
		producer.close();
    }
    private void broadcastContentToAA() {
    	//Assign topicName to string variable
		String topicName = "CC_TO_AA"; 
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
    	Producer<String, List<String>> producer = new KafkaProducer<String, List<String>>(props);
	   
		producer.send(new ProducerRecord<String, List<String>>(topicName,"1", l));//key,value
		System.out.println("Content sent to AA successfully");
		producer.close();
    }
    private String pollCollectiveSig() {
        String topicName = "AA_TO_CC";
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
    private void commit(String sig,String hash) {
    	//signature + hash of the content
    	return;
    }
}	