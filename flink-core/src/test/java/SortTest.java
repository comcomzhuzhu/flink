import java.util.ArrayList;

/**
 * @ClassName SortTest
 * @Description TODO
 * @Author Xing
 * @Date 2021/4/19 18:31
 * @Version 1.0
 */
public class SortTest {
    public static void main(String[] args) {
        ArrayList<Double> doubles = new ArrayList<>();
        doubles.add(0.1);
        doubles.add(0.2);
        doubles.add(0.05);
        doubles.add(0.2);
        doubles.sort(((o1, o2) -> -o1.compareTo(o2)));
        System.out.println(doubles);

        ArrayList<String> objects = new ArrayList<>();

        System.out.println(objects.size());

    }
}
