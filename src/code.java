import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;

/**
 * @ClassName code
 * @Description TODO
 * @Author Xing
 * @Version 1.0
 */
public class code {
    public static void main(String[] args) throws FileNotFoundException {
        File file=new File("D:\\WorkSpace\\Projects\\flink\\flink-core");
        filesDirs(file);
    }


    //使用递归遍历文件夹及子文件夹中文件
    public static void filesDirs(File file) throws FileNotFoundException {
        //File对象是文件或文件夹的路径，第一层判断路径是否为空
        if(file!=null){
            //第二层路径不为空，判断是文件夹还是文件
            if(file.isDirectory()){
                //进入这里说明为文件夹，此时需要获得当前文件夹下所有文件，包括目录
                File[] files=file.listFiles();//注意:这里只能用listFiles()，不能使用list()
                //files下的所有内容，可能是文件夹，也可能是文件，那么需要一个个去判断是文件还是文件夹，这个判断过程就是这里封装的方法
                //因此可以调用自己来判断，实现递归
                for (File flies2:files) {
                    filesDirs(flies2);
                }
            }else{
                System.out.println("文件名字"+file);

                FileReader fileReader = new FileReader(file);
            }
        }else{
            System.out.println("文件不存在");
        }
    }
}