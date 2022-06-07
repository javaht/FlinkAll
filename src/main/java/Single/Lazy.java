package Single;

public class Lazy {
    private  Lazy(){}

    private   static Lazy  lazy = null;

    public static  Lazy getInstance(){
          if(lazy==null){
              synchronized (Lazy.class){
                  if(lazy==null){
                      lazy =new Lazy();
                  }
              }
          }
          return lazy;
    }

}
