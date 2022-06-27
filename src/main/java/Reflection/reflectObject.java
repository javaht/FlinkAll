package Reflection;
/*
 * @Author root
 * @Data  2022/6/8 13:25
 * @Description
 * */


public class reflectObject {
    public static void main(String[] args) throws ClassNotFoundException, InstantiationException, IllegalAccessException {
        //获得class对象
        Class c1 = Class.forName("Reflection.Student");


        User user = (User) c1.newInstance();//本质是无参构造器
        System.out.println(user);
        //通过构造器创建对象
        c1.getDeclaredAnnotations();
    }
}

