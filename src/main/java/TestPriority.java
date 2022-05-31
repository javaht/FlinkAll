
/*
 * @Author root
 * @Data  2022/5/31 16:10
 * @Description
 * */



/*
* 优先级低只是意味着获得调度的概率低 并不是优先级低就不会被调用了 这都看cpu的调度
* */
public class TestPriority {
    public static void main(String[] args) {
        System.out.println("这是主线程的优先级"+Thread.currentThread().getName()+"------->"+Thread.currentThread().getPriority());

        MyPriority myPriority = new MyPriority();

        Thread t1 = new Thread(myPriority);
        Thread t2 = new Thread(myPriority);
        Thread t3 = new Thread(myPriority);
        Thread t4 = new Thread(myPriority);
        Thread t5 = new Thread(myPriority);
        Thread t6 = new Thread(myPriority);
     //先设置优先级  再启动线程

        t1.start();


        t2.setPriority(1);
        t2.start();

        t3.setPriority(4);
        t3.start();


        t4.setPriority(Thread.MAX_PRIORITY);
        t4.start();

/*        t5.setPriority(-1);
        t5.start();

           t6.setPriority(11);
           t6.start();*/

    }
}

class MyPriority implements Runnable {

    @Override
    public void run() {
        System.out.println(Thread.currentThread().getName()+"------->"+Thread.currentThread().getPriority());
    }
}
