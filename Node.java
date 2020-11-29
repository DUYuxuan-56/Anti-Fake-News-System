import java.io.IOException;
import java.math.BigInteger;

public class Node {

	public String nodeId;
    private final String _hostname;
    private final String _port;
    private final int seckeyNum;

 	Node(String hostname, String port) {

        this.nodeId = hostname + port;
        _hostname = hostname;
        _port = port;
        seckeyNum=_hostname.hashCode();
 	}
	public String getHostname() {
        return _hostname;
    }

    public String getPort() {
        return _port;
    }
    public int getSecKey() {
        return seckeyNum;
    }
    
}