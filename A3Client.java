import java.io.*;
import java.net.*;
import java.util.*;
import java.util.concurrent.atomic.*;

import org.slf4j.LoggerFactory;
import java.util.Properties;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.net.InetAddress;
import org.apache.log4j.BasicConfigurator;
import org.apache.log4j.Logger;

import org.apache.thrift.TProcessorFactory;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.server.TThreadPoolServer;
import org.apache.thrift.transport.TServerSocket;
import org.apache.thrift.transport.TFramedTransport;
import org.apache.thrift.transport.TNonblockingSocket;
import org.apache.thrift.transport.TTransportException;
import org.apache.thrift.async.AsyncMethodCallback;

import java.util.concurrent.CountDownLatch;

import org.apache.thrift.*;
import org.apache.thrift.transport.*;
import org.apache.thrift.protocol.*;

import org.apache.zookeeper.*;
import org.apache.zookeeper.data.*;
import org.apache.curator.*;
import org.apache.curator.retry.*;
import org.apache.curator.framework.*;
import org.apache.curator.framework.api.*;

import org.apache.log4j.*;

import ca.uwaterloo.watca.ExecutionLogger;

public class A3Client implements CuratorWatcher {
	static Logger log;

	String zkConnectString;
	String zkNode;
	int numThreads;
	int numSeconds;
	int keySpaceSize;
	CuratorFramework curClient;
	volatile boolean done = false;
	AtomicInteger globalNumOps;
	volatile InetSocketAddress primaryAddress;
	ExecutionLogger exlog;

	public static void main(String[] args) throws Exception {
		if (args.length != 5) {
			System.err.println("Usage: java A3Client zkconnectstring zknode num_threads num_seconds keyspace_size");
			System.exit(-1);
		}

		BasicConfigurator.configure();
		log = Logger.getLogger(A3Client.class.getName());

		A3Client client = new A3Client(args[0], args[1], Integer.parseInt(args[2]), Integer.parseInt(args[3]),
				Integer.parseInt(args[4]));

		try {
			client.start();
			client.execute();
		} catch (Exception e) {
			log.error("Uncaught exception", e);
		} finally {
			client.stop();
		}
	}

	A3Client(String zkConnectString, String zkNode, int numThreads, int numSeconds, int keySpaceSize) {
		this.zkConnectString = zkConnectString;
		this.zkNode = zkNode;
		this.numThreads = numThreads;
		this.numSeconds = numSeconds;
		this.keySpaceSize = keySpaceSize;
		globalNumOps = new AtomicInteger();
		primaryAddress = null;
		exlog = new ExecutionLogger("execution.log");
	}

	void start() {
		curClient = CuratorFrameworkFactory.builder().connectString(zkConnectString)
				.retryPolicy(new RetryNTimes(10, 1000)).connectionTimeoutMs(1000).sessionTimeoutMs(10000).build();

		curClient.start();
		exlog.start();
	}

	void execute() throws Exception {
		primaryAddress = getPrimary();
		String content="hallo";//pollContent();
		while (true) {
			try {
				KeyValueService.Client client = getThriftClient();
				String resp = client.collectiveSigning(content);
				System.out.println("Done with Signature " + resp);
				break;
			} catch (Exception e) {
				log.error("Exception during collective Signing");
				Thread.sleep(100);
				KeyValueService.Client client = getThriftClient();
			}
		}
		//sendCollectiveSigToCC(collectiveSig);
		/*List<Thread> tlist = new ArrayList<>();
		List<MyRunnable> rlist = new ArrayList<>();
		for (int i = 0; i < numThreads; i++) {
			MyRunnable r = new MyRunnable();
			Thread t = new Thread(r);
			tlist.add(t);
			rlist.add(r);
		}
		long startTime = System.currentTimeMillis();
		for (int i = 0; i < numThreads; i++) {
			tlist.get(i).start();
		}
		log.info("Done starting " + numThreads + " threads...");
		System.out.println("Done starting " + numThreads + " threads...");
		Thread.sleep(numSeconds * 1000);
		done = true;
		for (Thread t : tlist) {
			t.join(1000);
		}
		long estimatedTime = System.currentTimeMillis() - startTime;
		int tput = (int) (1000f * globalNumOps.get() / estimatedTime);
		System.out.println("Aggregate throughput: " + tput + " RPCs/s");
		long totalLatency = 0;
		for (MyRunnable r : rlist) {
			totalLatency += r.getTotalTime();
		}
		double avgLatency = (double) totalLatency / globalNumOps.get() / 1000;
		System.out.println("Average latency: " + ((int) (avgLatency * 100)) / 100f + " ms");*/
	}

	void stop() {
		curClient.close();
		exlog.stop();
	}

	InetSocketAddress getPrimary() throws Exception {
		while (true) {
			curClient.sync();
			List<String> children = curClient.getChildren().usingWatcher(this).forPath(zkNode);
			if (children.size() == 0) {
				log.error("No primary found");
				Thread.sleep(100);
				continue;
			}
			Collections.sort(children);
			byte[] data = curClient.getData().forPath(zkNode + "/" + children.get(0));
			String strData = new String(data);
			String[] primary = strData.split(":");
			log.info("Found primary " + strData);
			return new InetSocketAddress(primary[0], Integer.parseInt(primary[1]));
		}
	}

	KeyValueService.Client getThriftClient() {
		while (true) {
			try {
				TSocket sock = new TSocket(primaryAddress.getHostName(), primaryAddress.getPort());
				TTransport transport = new TFramedTransport(sock);
				transport.open();
				TProtocol protocol = new TBinaryProtocol(transport);
				return new KeyValueService.Client(protocol);
			} catch (Exception e) {
				log.error("Unable to connect to primary");
			}
			try {
				Thread.sleep(100);
			} catch (InterruptedException e) {
			}
		}
	}

	synchronized public void process(WatchedEvent event) {
		log.info("ZooKeeper event " + event);
		try {
			primaryAddress = getPrimary();
		} catch (Exception e) {
			log.error("Unable to determine primary");
		}

	}

	class MyRunnable implements Runnable {
		long totalTime;
		KeyValueService.Client client;

		MyRunnable() throws TException {
			client = getThriftClient();
		}

		long getTotalTime() {
			return totalTime;
		}

		public void run() {
			totalTime = 0;
			long tid = Thread.currentThread().getId();
			int numOps = 0;
			try {
				while (!done) {
					long startTime = System.nanoTime();
					
					while (!done) {
						try {
							String content ="";
							exlog.logReadInvocation(tid, content);
							String resp = client.collectiveSigning(content);
							exlog.logReadResponse(tid, content, resp);
							numOps++;
							break;
						} catch (Exception e) {
							log.error("Exception during get");
							Thread.sleep(100);
							client = getThriftClient();
						}
					}
					
					long diffTime = System.nanoTime() - startTime;
					totalTime += diffTime / 1000;
				}
			} catch (Exception x) {
				x.printStackTrace();
			}
			globalNumOps.addAndGet(numOps);
		}
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
