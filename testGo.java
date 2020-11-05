import com.sun.jna.*;
import java.util.*;

public class testGo {
  public interface Cosi extends Library {
  	public class GoString extends Structure {
  	  public static class ByValue extends GoString implements Structure.ByValue {}
      public String p;
      public long n;
      protected List getFieldOrder(){
        return Arrays.asList(new String[]{"p","n"});
	  }

    }
    void TestCoSi(GoString.ByValue m,long a, long b);
  }
  static public void main(String argv[]) {
    Cosi cosi = (Cosi) Native.load(
      "./cosi.so", Cosi.class);

    Cosi.GoString.ByValue message = new Cosi.GoString.ByValue();
    message.p = "hallo";
    message.n = message.p.length();
    cosi.TestCoSi(message,5, 1);
  }
}