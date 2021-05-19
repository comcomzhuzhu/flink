/**
 * @ClassName Test
 * @Description TODO
 * @Author Xing
 * 11 11:35
 * @Version 1.0
 */
public class Test {
    public static void main(String[] args) {
        MyTest myTest = new MyTest();
        System.out.println(myTest.hashCode());
        test(myTest);
    }

    private static void test(Runnable r) {
        r.run();
    }
}

class MyTest implements Runnable {
    @Override
    public void run() {
        System.out.println(this.hashCode());
    }
}
