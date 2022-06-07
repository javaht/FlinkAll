package AllLock;
/*
 * @Author root
 * @Data  2022/6/7 13:38
 * @Description
 * */


import java.util.concurrent.locks.ReentrantLock;

public class ReentrantLockTest {

    public static void main(String[] args) {
        Ticket ticket = new Ticket();

        new Thread(()->{
            for (int i = 0; i < 300; i++) {
                ticket.sale();
            }
        },"A====>").start();

        new Thread(()->{
            for (int i = 0; i < 300; i++) {
                ticket.sale();
            }
        },"B====>").start();





    }


}

class Ticket{
    private int ticketNum=500;

    ReentrantLock lock = new ReentrantLock();

     public  void sale(){
         lock.lock();
         try {
             while(ticketNum>0){
              System.out.println(Thread.currentThread().getName()+"卖出第"+ticketNum+"张票");
              ticketNum--;
           }
         } catch (Exception e) {
             e.printStackTrace();
         } finally {
             lock.unlock();
         }

     }

}



