import java.util.ArrayList;

/**
 * @ClassName LIstAdd
 * @Description TODO
 * @Author Xing
 * @Version 1.0
 */
public class LIstAdd {
    public static void main(String[] args) {
        ArrayList<Integer> arrayList = new ArrayList<>();
        arrayList.add(1);
        arrayList.add(2);
        int k = 3;
        int j= arrayList.size();
        for (int i = j ; i > 0; i--) {
            if (k > arrayList.get(i-1)) {
                arrayList.add(i, k);
                System.out.println(arrayList);
            }
        }
        System.out.println(arrayList);
    }
}
