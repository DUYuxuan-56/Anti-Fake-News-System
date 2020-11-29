import java.util.*;
import org.mindrot.jbcrypt.BCrypt;
import java.util.Random;
import java.math.BigInteger;

public class SignatureServiceHandler implements BcryptService.Iface {

	public String bytesToHex(byte[] bytes) {
		char[] hexArray = "0123456789ABCDEF".toCharArray();
	    char[] hexChars = new char[bytes.length * 2];
	    for ( int j = 0; j < bytes.length; j++ ) {
	        int v = bytes[j] & 0xFF;
	        hexChars[j * 2] = hexArray[v >>> 4];
	        hexChars[j * 2 + 1] = hexArray[v & 0x0F];
	    }
	    return new String(hexChars);
	}

	public String signature(String content,int seckeyNum) throws IllegalArgument
	{
		System.out.println("get sign task"+" , res is "+ bytesToHex(Schnorr.schnorr_sign(content.getBytes(), BigInteger.valueOf(seckeyNum))));
        return bytesToHex(Schnorr.schnorr_sign(content.getBytes(), BigInteger.valueOf(seckeyNum)));

	}
	public void checkAndUpdateMap(String hostname, String port) throws IllegalArgument{
		try {
			String nodeId=hostname + port;
			if(!NodeManager.containID(nodeId)){
				Node node = new Node(hostname, port);
              	NodeManager.addNode(nodeId, node);
                ClientPool.addNodes(hostname, port);
			}
		} catch (Exception e) {
			throw new IllegalArgument(e.getMessage());
		}
	}
}
