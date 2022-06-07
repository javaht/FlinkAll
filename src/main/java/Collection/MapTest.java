package Collection;
/*
 * @Author root
 * @Data  2022/6/7 15:53
 * @Description
 * */


import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

public class MapTest {

    public static void main(String[] args) {
        //map是这样用的吗？ 不是，工作中不使用HashMap
        //默认等价于什么？ new HashMap<>(16,0.75);
        //Map<String, String> map = new HashMap<>();

        // Collections.synchronizedMap()
        //Map<String, String> map = Collections.synchronizedMap(new HashMap<>());
        Map<String, String> map = new ConcurrentHashMap<>();


        for (int i = 1; i <= 30; i++) {
            new Thread(()->{
                map.put(Thread.currentThread().getName(), UUID.randomUUID().toString().substring(0,5));
                System.out.println(map);
            },String.valueOf(i)).start();
        }
    }
}
