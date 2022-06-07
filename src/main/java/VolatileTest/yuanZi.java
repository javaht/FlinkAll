package VolatileTest;

import java.util.concurrent.atomic.AtomicInteger;

public class yuanZi {

        /**
         *  volatile 不保证原子性   如何避免？
         */
        private volatile static int num = 0;
      //  private static AtomicInteger num = new AtomicInteger();

        public static void add(){
            num++;
            // num.getAndIncrement();    AtomicInteger的+1操作
        }

        public static void main(String[] args) {
            //理论上结果为20000
            for (int i = 1; i <= 20; i++) {
                new Thread(()->{
                    for (int j = 0; j < 1000; j++) {
                        add();
                    }
                }).start();
            }
            while (Thread.activeCount() > 2){
                Thread.yield();
            }

            System.out.println(Thread.currentThread().getName()+" "+num);
    }

}
