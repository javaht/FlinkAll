package Single;

/*
* 静态内部类构造
* */
public class staticClass {

    private staticClass(){}

    public static   staticClass getInstance(){
        return Test.staticC;
    }

    public static class Test{
        private  static  final staticClass staticC = new  staticClass();
    }

}
