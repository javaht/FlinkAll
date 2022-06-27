package Thread.CAS;

import java.util.concurrent.atomic.AtomicInteger;

public class CASDemo {
    public static void main(String[] args) {
        AtomicInteger atomicInteger = new AtomicInteger(2022);

        // 期望值 ，更新值
        // public final boolean compareAndSet(int expect, int update)
        // 如果我期望的值达到了，那么就更新，否则，就不更新 ,CAS 是CPU的并发原语
        System.out.println(atomicInteger.compareAndSet(2022, 2023));
        System.out.println(atomicInteger.get());
        atomicInteger.getAndIncrement();
        System.out.println(atomicInteger.compareAndSet(2022, 2023));
    }
}
