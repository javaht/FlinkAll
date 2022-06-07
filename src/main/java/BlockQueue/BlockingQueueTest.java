package BlockQueue;
/*
 * @Author root
 * @Data  2022/6/7 16:25
 * @Description
 * */


import java.util.concurrent.ArrayBlockingQueue;

public class BlockingQueueTest {
    public static void main(String[] args) {

        ArrayBlockingQueue<String> block = new ArrayBlockingQueue<String>(3);

 /*       block.add("1");
        block.add("2");
        block.add("3");

        System.out.println("========================");
        // java.util.NoSuchElementException
        System.out.println(block.remove());
        System.out.println(block.remove());
        System.out.println(block.remove());
        System.out.println(block.remove());*/
/*
        block.offer("1");
        block.offer("2");
        block.offer("3");

        System.out.println(block.remove());
        System.out.println(block.poll());
        System.out.println(block.poll());
        System.out.println(block.poll());*/




    }
}
