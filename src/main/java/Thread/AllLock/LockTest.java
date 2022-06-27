package Thread.AllLock;

import java.util.concurrent.locks.ReentrantLock;

public class LockTest {
    public static void main(String[] args) {

        TestLock testLock = new TestLock();
        new Thread(testLock,"1").start();
        new Thread(testLock,"2").start();
        new Thread(testLock,"3").start();

    }




 }

class TestLock implements Runnable{
    int ticknumber = 1000;
    //定义lock锁
       private final  ReentrantLock lock =   new ReentrantLock();

    @Override
    public void run() {
        while(true){
            try{
                lock.lock();//枷锁
                if(ticknumber>0){
                    try {
                        Thread.sleep(1);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                    System.out.println(ticknumber--);
                }else{
                    break;
                }
            }finally {
                lock.unlock();
            }



        }
    }
}