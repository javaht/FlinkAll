import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicStampedReference;

public class atomicReferenceDemo {
    public static void main(String[] args) {
        // AtomicStampedReference 注意：如果泛型是一个包装类，注意对象的引用问题
        // 正常的业务操作，这里引用的都是一个个对象
        AtomicStampedReference<Integer> atomicStampedReference = new AtomicStampedReference<Integer>(1,1);

        // CAS compareAndSet: 比较并交换！
        // 乐观锁的原理一样！
        new Thread(()->{
            //获取版本号
            int stamp = atomicStampedReference.getStamp();
            System.out.println("a1 =>"+stamp);
            try {
                TimeUnit.SECONDS.sleep(1);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            System.out.println("a2里面的"+atomicStampedReference.compareAndSet(1, 2, atomicStampedReference.getStamp(), atomicStampedReference.getStamp() + 1));
            System.out.println("a2 =>"+stamp);

            System.out.println("a3里面的"+atomicStampedReference.compareAndSet(2, 1, atomicStampedReference.getStamp(), atomicStampedReference.getStamp() + 1));
            System.out.println("a3 =>"+stamp);

        },"a").start();


        new Thread(()->{
            //获取版本号
            int stamp = atomicStampedReference.getStamp();
            System.out.println("b1 =>"+stamp);

            try {
                TimeUnit.SECONDS.sleep(2);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }

            System.out.println("b2里的"+atomicStampedReference.compareAndSet(1, 6, stamp, stamp + 1));
            System.out.println("b2 =>"+atomicStampedReference.getStamp());

        },"b").start();
    }
}
