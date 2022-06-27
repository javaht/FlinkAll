package Reflection;
/*
 * @Author root
 * @Data  2022/6/8 10:46
 * @Description
 * */



public class createObject {
    public static void main(String[] args) throws ClassNotFoundException {

        //1.正常new
        Person person  =new Student();
        System.out.println(person.hashCode());

        Class c1 = person.getClass();
        System.out.println(c1.hashCode());
        //2.反射new
        Class c2 = Class.forName("Reflection.Student");
        System.out.println(c2.hashCode());
 
        //3.类名
        Class c3 = Student.class;
        System.out.println(c3.hashCode());

    }
}


class Person{
 public String name;
    public Person() {
    }
    public Person(String name) {
        this.name = name;
    }
    @Override
    public String toString() {
        return "Person{" +
                "name='" + name + '\'' +
                '}';
    }
}

class Student extends Person{
    public Student(){
        this.name="学生";
      }
}

class Teacher extends Person{
    public Teacher(){
        this.name="老师";
    }
}
