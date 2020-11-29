import java.net.InetAddress;
import org.apache.log4j.BasicConfigurator;
import org.apache.log4j.Logger;
import org.mindrot.jbcrypt.BCrypt;

import java.net.*;
import java.util.*;
import org.apache.thrift.TProcessorFactory;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.server.TSimpleServer;
import org.apache.thrift.server.TThreadPoolServer;
import org.apache.thrift.transport.TServerSocket;
import org.apache.thrift.transport.TFramedTransport;
import org.apache.thrift.transport.TNonblockingSocket;
import org.apache.thrift.transport.TTransportException;

import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.protocol.TProtocol;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.zookeeper.*;
import org.apache.zookeeper.data.*;
import org.apache.curator.*;
import org.apache.curator.retry.*;
import org.apache.curator.framework.*;
import org.apache.curator.framework.api.*;


public class WorkerAA implements CuratorWatcher{

	static Logger log;
	volatile InetSocketAddress primaryAddress;
	CuratorFramework curClient;
	static String hostWorkerAA;
	static int portWorkerAA;
	static String zkNode;
	private static ExecutorService signatureService = Executors.newFixedThreadPool(1);

	public static void main(String [] args) throws Exception {
		if (args.length != 4) {
			System.err.println("Usage: java WorkerAA MasterAA_host MasterAA_port WorkerAA_port zkNode");
			System.exit(-1);
		}

		// initialize log4j
		BasicConfigurator.configure();
		log = Logger.getLogger(WorkerAA.class.getName());

		String hostMasterAA = args[0];
		hostWorkerAA = getHostName();
		int portMasterAA = Integer.parseInt(args[1]);
		portWorkerAA = Integer.parseInt(args[2]);
		zkNode=args[3];
		log.info("Launching WorkerAA node on port " + portWorkerAA + " at host " + getHostName());

		signatureService.execute(new LaunchServer(portWorkerAA));
		Thread.sleep(300);

		WorkerAA zkclient = new WorkerAA();
		try {
			zkclient.start();
			zkclient.execute();
		} catch (Exception e) {
			log.error("Uncaught exception", e);
		}
        
	}
	void start() {
		String zkConnectString="manta.uwaterloo.ca:2181";
		curClient = CuratorFrameworkFactory.builder().connectString(zkConnectString)
				.retryPolicy(new RetryNTimes(10, 1000)).connectionTimeoutMs(1000).sessionTimeoutMs(10000).build();

		curClient.start();
	}
	void execute() throws Exception{
		primaryAddress = getPrimary();
		// Create a client to the MasterAA
		TTransport transport =null;
		System.out.println("primaryAddress hostname"+primaryAddress.getHostName()+" primaryAddress port"+primaryAddress.getPort());
		while (true) {
			try {
				TSocket sock = new TSocket(primaryAddress.getHostName(), 10000);//10000 is always for worker to connect
				transport = new TFramedTransport(sock);
				transport.open();
				TProtocol protocol = new TBinaryProtocol(transport);
				BcryptService.Client client = new BcryptService.Client(protocol);
				client.checkAndUpdateMap(hostWorkerAA,String.valueOf(portWorkerAA) );
				transport.close();

                System.out.println("heartBeat to MasterAA");
			} catch (Exception e) {
				System.out.println(e.getMessage());
				log.error("Unable to connect to primary");
			}finally {
                if(transport.isOpen()){
                    transport.close();
                }
            }
			try {
				Thread.sleep(1000);
			} catch (InterruptedException e) {
			}
		}
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
	synchronized public void process(WatchedEvent event) {
		log.info("ZooKeeper event " + event);
		try {
			primaryAddress = getPrimary();
		} catch (Exception e) {
			log.error("Unable to determine primary");
		}

	}
	static class LaunchServer implements Runnable {

		private int _portWorkerAA;

        public LaunchServer(int portWorkerAA) {
            _portWorkerAA = portWorkerAA;
        }

        @Override
        public void run() {
        	// launch Thrift server
			BcryptService.Processor processor = new BcryptService.Processor<BcryptService.Iface>(new SignatureServiceHandler());
			TServerSocket socket=null;
			try{
			 	socket = new TServerSocket(_portWorkerAA);
			}catch(TTransportException e){
				System.out.println(e.getMessage());
			 }
			TThreadPoolServer.Args sargs = new TThreadPoolServer.Args(socket);
			sargs.protocolFactory(new TBinaryProtocol.Factory());
			sargs.transportFactory(new TFramedTransport.Factory());
			sargs.processorFactory(new TProcessorFactory(processor));
			//sargs.maxWorkerThreads(48);
			TThreadPoolServer server = new TThreadPoolServer(sargs);
			server.serve();
        }
    }

	static String getHostName()
	{
		try {
			return InetAddress.getLocalHost().getHostName();
		} catch (Exception e) {
			System.out.println(e.getMessage());
			return "localhost";
		}
	}
}
