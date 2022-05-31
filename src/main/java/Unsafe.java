import lombok.SneakyThrows;

public class Unsafe {
    public static void main(String[] args) {

        BuyTicket buyTicket = new BuyTicket();
        new Thread(buyTicket,"2").start();
        new Thread(buyTicket,"3").start();
        new Thread(buyTicket,"1").start();

    }

}

class BuyTicket  implements Runnable{

    private int ticknumber = 100;
    boolean flag = true;
    @SneakyThrows
    @Override
    public void run() {
         while(flag){
             buy();
         }
    }
    private synchronized  void buy() throws InterruptedException {
        if(ticknumber<=0){
            flag = false;
            return;
        }

        Thread.sleep(100);

        System.out.println(Thread.currentThread().getName()+"拿到票"+ticknumber--);
    }

}