package Single;

public class Hungry {
    private  Hungry(){}

    private  static final  Hungry hungry = new Hungry();

    public static  Hungry getInstance(){
        return hungry;
    }

}
