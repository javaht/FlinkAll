package Reflection;
/*
 * @Author root
 * @Data  2022/6/8 10:00
 * @Description
 * */


import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/*
* 反射获取
* */
public class Test01 {
    public static void main(String[] args) throws ClassNotFoundException {
        Class c1 = Class.forName("Reflection.Test01");
        Class c2 = Class.forName("Reflection.Test01");
        Class c3 = Class.forName("Reflection.Test01");


        /*
        * 一个类在内存中只有一个class对象  被加载后类的整个机构都被封装在class对象中
        * */
        System.out.println(c1 == c2);
        System.out.println(c1 == c3);
     }
}


@Data
@AllArgsConstructor
@NoArgsConstructor
class User{
    private String name;
    private int age;
    private int id;
}
