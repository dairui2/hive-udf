package udf;


import java.lang.reflect.Constructor;

public class ReflectTest {
    public static void main(String[] args) throws Exception {

        Class clazz = null;
        clazz = Class.forName("com.jas.reflect.Fruit");
        Constructor<Fruit> constructor1 = clazz.getConstructor();
        Constructor<Fruit> constructor2 = clazz.getConstructor(String.class);

        Fruit fruit1 = constructor1.newInstance();
        Fruit fruit2 = constructor2.newInstance("Apple");

    }
}

class Fruit{
    public Fruit(){
        System.out.println("无参构造器 Run...........");
    }
    public Fruit(String type){
        System.out.println("有参构造器 Run..........." + type);
    }

}