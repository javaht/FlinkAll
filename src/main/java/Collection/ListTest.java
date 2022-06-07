package Collection;
/*
 * @Author root
 * @Data  2022/6/7 15:22
 * @Description
 * */

import java.util.*;
import java.util.concurrent.CopyOnWriteArrayList;

public class ListTest {
    public static void main(String[] args){
        List<String> list = new CopyOnWriteArrayList<>();


        List<String> list2 = Collections.synchronizedList(new ArrayList<>());


        new HashSet<>();

        for(int i = 1;i<=30;++i){
            new Thread(()->{
                list.add(UUID.randomUUID().toString().substring(0,5));
            },String.valueOf(i)).start();
        }
    }
}
