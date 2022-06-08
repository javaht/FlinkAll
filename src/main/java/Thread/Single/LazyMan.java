package Thread.Single;
/*
 * @Author root
 * @Data  2022/6/8 14:17
 * @Description
 * */


public class LazyMan {

    private LazyMan(){
        System.out.println(Thread.currentThread().getName());
    }

    private static volatile LazyMan LAZYMAN;

    //双重检测锁模式，懒汉式单例，DCL懒汉式
    public static LazyMan getInstance(){
        if (LAZYMAN==null){
            synchronized (LazyMan.class){//这里的同步锁 类名.class是什么意思？
                if (LAZYMAN == null){
                    LAZYMAN = new LazyMan();//不是原子性操作
                    /**
                     * 1.分配内存空间
                     * 2.执行构造方法，初始化对象
                     * 3.把这个对象指向这个操作
                     * 在这些操作中，存在指令重排，我们希望是123，可能执行的时候是132
                     * 所以为了避免指令重排，需要在对象LAZYMAN加上volatile关键字，来避免指令重排
                     *
                     */
                }
            }
        }
        return LAZYMAN;
    }

    //以上的代码，在单线程下是没有问题的，但是在多线程下是有问题，以下是测试
    public static void main(String[] args) {
        for (int i = 0; i < 10; i++) {
            new Thread(() -> {
                LazyMan.getInstance();
            }).start();
        }
    }
}