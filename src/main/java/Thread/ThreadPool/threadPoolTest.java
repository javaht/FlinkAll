package Thread.ThreadPool;
/*
 * @Author root
 * @Data  2022/6/7 16:35
 * @Description
 * */


import java.util.concurrent.*;

public class threadPoolTest {
    public static void main(String[] args) {
       // ExecutorService threadPool = Executors.newSingleThreadExecutor();
        //ExecutorService threadPool = Executors.newFixedThreadPool(5);
       // ExecutorService threadPool = Executors.newCachedThreadPool();


        ExecutorService threadPool =  new ThreadPoolExecutor(
                2,
                5,
                3
                , TimeUnit.SECONDS,
                new ArrayBlockingQueue<Runnable>(3),
                Executors.defaultThreadFactory(),
                new ThreadPoolExecutor.DiscardOldestPolicy()  //拒绝策略
        );



        try {
            for (int i = 0; i < 5; i++) {
                threadPool.execute(()->{
                    System.out.println(Thread.currentThread().getName()+"======="+"执行了");
                });
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            threadPool.shutdown();
        }


    }
}
