
/*
 * @Author root
 * @Data  2022/5/31 12:21
 * @Description
 * */


public class ThreadTest {
    public static void main(String[] args) throws InterruptedException {
  Thread  thread = new Thread(()->{
      for (int i = 0; i < 5; i++) {
          try {
              Thread.sleep(1000);
              System.out.println("这是测试");
          } catch (InterruptedException e) {
              e.printStackTrace();
          }
      }
      System.out.println("//////////////");
  });


  Thread.State state = thread.getState();
        System.out.println("这是new  "+state);


        thread.start();
        state = thread.getState();
        System.out.println("启动后的"+state);

        while(state!=Thread.State.TERMINATED){
            Thread.sleep(100);
            state = thread.getState();
            System.out.println("等待中的"+state);

        }
    }



}
