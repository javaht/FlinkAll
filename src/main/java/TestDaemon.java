public class TestDaemon {

    public static void main(String[] args) {
        God god = new God();
        You you = new You();


        Thread thread = new Thread(god);
        thread.setDaemon(true);//设置为守护线程


        thread.start();

        new Thread(you).start();
    }

}


class God implements Runnable{

    @Override
    public void run() {
        while (true){
            System.out.println("上帝一直跑");
        }
    }
}

class You implements Runnable {
    @Override
    public  void run() {
        for (int i = 0; i < 36500; i++) {
            System.out.println("一生都开心的或者");
        }
        System.out.println("------------goodbye------------");
    }
}