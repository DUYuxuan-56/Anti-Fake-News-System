import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TFramedTransport;
import org.apache.thrift.transport.TNonblockingTransport;
import org.apache.thrift.transport.TNonblockingSocket;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.async.TAsyncClientManager;
import org.apache.thrift.protocol.TProtocolFactory;
import java.io.IOException;
import org.mindrot.jbcrypt.BCrypt;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.math.BigInteger;

public class ClientPool {

    private static ConcurrentHashMap<BcryptService.AsyncClient, Boolean> clientLock = new ConcurrentHashMap<>();
    private static ConcurrentHashMap<String, ArrayList<BcryptService.AsyncClient>> clientSet = new ConcurrentHashMap<>();
	
	public static synchronized BcryptService.AsyncClient getClient(Node node){
        
        String nodeId=node.getHostname() + node.getPort();
        while(true){
            for(BcryptService.AsyncClient client:clientSet.get(nodeId)){
                if(!locked(client)){
                    lock(client);
                    return client;
                }
            }
            addNodes(node.getHostname(),node.getPort());
        }
	}
    public static synchronized int getSecKey(Node node){
        return node.getSecKey();
    }
	public static synchronized Boolean locked(BcryptService.AsyncClient client) {
        return clientLock.get(client);
    }
    public static synchronized void lock(BcryptService.AsyncClient client) {
        clientLock.put(client,true);
    }
    public static synchronized void unlock(BcryptService.AsyncClient client) {
        clientLock.put(client,false);
    }
    public static synchronized void addNodes(String hostname, String port) {

        String nodeId=hostname + port;
        clientSet.put(nodeId,new ArrayList<BcryptService.AsyncClient>());
        for(int i=0;i<3;i++){
            TNonblockingTransport transport = null;
            TAsyncClientManager clientManager = null;
            TProtocolFactory protocolFactory = null;
            BcryptService.AsyncClient client = null;
            try{
                transport = new TNonblockingSocket(hostname, Integer.parseInt(port));
                clientManager = new TAsyncClientManager();
                protocolFactory = new TBinaryProtocol.Factory();
                client = new BcryptService.AsyncClient(protocolFactory, clientManager, transport);
                clientLock.put(client,false);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
            clientSet.get(nodeId).add(client);
        }
        System.out.println("add 3 more connections");
    }
}