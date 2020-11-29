import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TFramedTransport;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TNonblockingTransport;
import org.apache.thrift.transport.TNonblockingSocket;
import org.apache.thrift.async.TAsyncClientManager;
import org.apache.thrift.protocol.TProtocolFactory;

import java.util.concurrent.ConcurrentHashMap;
import java.util.Random;
import java.util.Iterator;
import java.util.Arrays;
import java.util.Comparator;
import java.util.*;
import java.io.IOException;

public class NodeManager {

	private static ConcurrentHashMap<String, Node> nodeMap = new ConcurrentHashMap<>();
    private static int int_random=0;

	public static Boolean containID(String nodeId){
		return nodeMap.containsKey(nodeId);
	}
	public static void addNode(String nodeId,Node node){
		nodeMap.put(nodeId,node);
	}
    public static Boolean hasBENodes() {
        return nodeMap.size()>0;
    }
    public static int numBENodes() {
        return nodeMap.size();
    }
    public static synchronized Object[] getOrderedNodeList() {
        Object[] nodes = nodeMap.values().toArray();
        Node[] nodeLs=new Node[nodes.length];
        for(int i=0;i<nodes.length;i++) {
            Node n=(Node)nodes[i];
            nodeLs[i]=n;
        }
        Arrays.sort(nodeLs, new Comparator<Node>() {
            @Override
            public int compare(Node o1, Node o2) {
                return o1.nodeId.compareTo(o2.nodeId);
            }
        });
        return nodeLs;
    }
}