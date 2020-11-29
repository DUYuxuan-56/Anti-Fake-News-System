import org.slf4j.LoggerFactory;
import java.util.Properties;
import java.util.*;
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
import org.apache.thrift.async.TAsyncClientManager;
import org.apache.thrift.async.AsyncMethodCallback;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.math.BigInteger;
import java.util.concurrent.*;
import java.io.*;
import java.net.*;
import java.util.concurrent.locks.*;
import java.util.concurrent.atomic.*;

import org.apache.thrift.*;
import org.apache.thrift.server.*;
import org.apache.thrift.transport.*;
import org.apache.thrift.protocol.*;

import org.apache.zookeeper.*;
import org.apache.zookeeper.data.*;
import org.apache.curator.*;
import org.apache.curator.retry.*;
import org.apache.curator.framework.*;
import org.apache.curator.framework.api.*;

import com.google.common.util.concurrent.Striped;

import org.apache.log4j.*;

public class KeyValueHandler implements KeyValueService.Iface, CuratorWatcher{
    private CuratorFramework curClient;
    private String zkNode;
    private String host;
    private int port;

    private static Logger log;
    private volatile Boolean isPrimary = false;
    private ReentrantLock globalLock = new ReentrantLock();
    private Striped<Lock> stripedLock = Striped.lock(64);
    private volatile ConcurrentLinkedQueue<KeyValueService.Client> backupClients = null;
    private int clientNumber = 1;

    public KeyValueHandler(String host, int port, CuratorFramework curClient, String zkNode) throws Exception {
        this.host = host;
        this.port = port;
        this.curClient = curClient;
        this.zkNode = zkNode;

        log = Logger.getLogger(KeyValueHandler.class.getName());
        // Set up watcher
        curClient.sync();
        List<String> children = curClient.getChildren().usingWatcher(this).forPath(zkNode);

        if (children.size() == 1) {
            // System.out.println("Is Primary: " + true);
            this.isPrimary = true;
        } else {
            // Find primary data and backup data
            Collections.sort(children);
            byte[] backupData = curClient.getData().forPath(zkNode + "/" + children.get(children.size() - 1));
            String strBackupData = new String(backupData);
            String[] backup = strBackupData.split(":");
            String backupHost = backup[0];
            int backupPort = Integer.parseInt(backup[1]);

            // Check if this is primary
            if (backupHost.equals(host) && backupPort == port) {
                // System.out.println("Is Primary: " + false);
                this.isPrimary = false;
            } else {
                // System.out.println("Is Primary: " + true);
                this.isPrimary = true;
            }
        }
    }

    public void setPrimary(boolean isPrimary) throws org.apache.thrift.TException {
        this.isPrimary = isPrimary;
    }

    public String collectiveSigning(String content) throws org.apache.thrift.TException {

        if (isPrimary == false) {
            // System.out.println("Backup is not allowed to get.");
            throw new org.apache.thrift.TException("Backup is not allowed to get.");
        }

        while(!NodeManager.hasBENodes()){
            //wait
            try {
                Thread.sleep(300);
            } catch (Exception ex) {
                // do nothing
            }
        }
        ArrayList<String> res ;
        while(true){
            Object[] nodeList=NodeManager.getOrderedNodeList();
            res = new ArrayList<String>();
            ArrayList<BcryptService.AsyncClient> clients = new ArrayList<BcryptService.AsyncClient>();
            ArrayList<SignatureCallback> signatureCallbacks = new ArrayList<SignatureCallback>();
            
            for(int i=0;i<nodeList.length;i++){

                try {
                    SignatureCallback signatureCallback = new SignatureCallback();
                    signatureCallbacks.add(signatureCallback);


                    TNonblockingTransport transport = null;
                    TAsyncClientManager clientManager = null;
                    TProtocolFactory protocolFactory = null;
                    BcryptService.AsyncClient client = null;
                    transport = new TNonblockingSocket(((Node)nodeList[i]).getHostname(), Integer.parseInt(((Node)nodeList[i]).getPort()));
                    clientManager = new TAsyncClientManager();
                    protocolFactory = new TBinaryProtocol.Factory();
                    client = new BcryptService.AsyncClient(protocolFactory, clientManager, transport);


                    //BcryptService.AsyncClient client = ClientPool.getClient((Node)nodeList[i]);
                    clients.add(client);
                    client.signature(content,ClientPool.getSecKey((Node)nodeList[i]),signatureCallback);
                    System.out.println("request sent");
                } catch (Exception e) {
                    System.out.println(e.getMessage());
                }
            }
            //wait all worker to respond
            while(checkCallBack(signatureCallbacks)=="notDone"){
                try {
                    Thread.sleep(20);
                } catch (Exception ex) {
                    // do nothing
                }
            }
            if(checkCallBack(signatureCallbacks)=="Error"){
                try {
                    Thread.sleep(2000);
                    System.out.println("some worker fails");
                } catch (Exception ex) {
                    // do nothing
                }
                continue;
            }
            for(int i=0;i<nodeList.length;i++){
                ClientPool.unlock(clients.get(i));
                String signature=signatureCallbacks.get(i).getResult();
                System.out.println("signature is "+signature);
                res.add(signature);
            }
            break;
        }

        String listString = "";

        for (String s : res)
        {
            listString += s + ",";
        }

        return listString;
    }
    public String checkCallBack(ArrayList<SignatureCallback> signatureCallbacks){
        for(int i=0;i<signatureCallbacks.size();i++){
            if(signatureCallbacks.get(i).getError()){
                return "Error";
            }
            if(!signatureCallbacks.get(i).getCompleted()){
                return "notDone";
            }
        }
        return "Done";

    }
    private class SignatureCallback implements AsyncMethodCallback<String> {

        private String result="";
        private boolean completed=false;
        private boolean error=false;
        @Override
        public void onComplete(String response) {
            try {
                this.result = response;
                completed=true;
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }
        @Override
        public void onError(Exception exception) {
            completed=true;;
        }

        public String getResult() {
            return this.result;
        }
        public Boolean getCompleted() {
            return completed;
        }
        public Boolean getError() {
            return error;
        }

    }
    
	synchronized public void process(WatchedEvent event) throws org.apache.thrift.TException {
        // Lock the entire hashmap on primary
        try {
            // Get all the children
            curClient.sync();
            List<String> children = curClient.getChildren().usingWatcher(this).forPath(zkNode);

            if (children.size() == 1) {
                System.out.println("Is Primary: " + true);
                this.isPrimary = true;
                return;
            }
            
            // Find primary data and backup data
            Collections.sort(children);
            byte[] backupData = curClient.getData().forPath(zkNode + "/" + children.get(children.size() - 1));
            String strBackupData = new String(backupData);
            String[] backup = strBackupData.split(":");
            String backupHost = backup[0];
            int backupPort = Integer.parseInt(backup[1]);

            // Check if this is primary
            if (backupHost.equals(host) && backupPort == port) {
                System.out.println("Is Primary: " + false);
                this.isPrimary = false;
            } else {
                System.out.println("Is Primary: " + true);
                this.isPrimary = true;
            }
            
            
        } catch (Exception e) {
            System.out.println(e.getMessage());
            log.error("Unable to determine primary or children");
            this.backupClients = null;
        }
    }
}
