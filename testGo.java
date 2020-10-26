import com.sun.jna.*;

public class testGo {
  public interface Cosi extends Library {
    void TestCoSi(String m,int a, int b);
  }
  static public void main(String argv[]) {
    Cosi cosi = (Cosi) Native.load(
      "./cosi.so", Cosi.class);
    String message="hallo";
    cosi.TestCoSi(message,3, 0);
  }
}