import java.util.Random;

/**
 * Created by ruanzf on 2016/8/25.
 */
public class Test {

    public static void main(String[] args) {
        for(int i=100; i<10000; i++) {
            Random random = new Random();
            int r = random.nextInt(10);
            if (r < 6) {
                System.out.println("linesum.com." + i);
            }else {
                System.out.println(i);
            }
        }
    }
}
