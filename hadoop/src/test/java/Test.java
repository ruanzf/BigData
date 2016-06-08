import java.util.ArrayList;
import java.util.List;

/**
 * Created by ruan on 2016/5/18.
 */
public class Test {

    public static void main(String[] args) {
        int i=0;
        List<BigObject> list = new ArrayList<BigObject>();
        while (true) {
            list.add(new BigObject());
            System.out.println(i++);
        }
    }


}
class BigObject{
    byte[] bytes = new byte[1024*1024]; //1M
}