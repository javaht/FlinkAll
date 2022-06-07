package VolatileTest;
/*
 * @Author root
 * @Data  2022/6/7 17:24
 * @Description
 * */


import java.util.concurrent.TimeUnit;

public class Jmm {
    /**
     *  不加volatile 程序就会死循环
     *  加volatile 可以保证可见性
     */
    private volatile static int num = 0;
    public static void main(String[] args) {//main线程

        // 线程1 对主内存的变化是不知道的
        new Thread(()->{
            while (num == 0){

            }
        }).start();

        try {
            TimeUnit.SECONDS.sleep(2);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        num = 1;
        System.out.println(num);
    }
}
