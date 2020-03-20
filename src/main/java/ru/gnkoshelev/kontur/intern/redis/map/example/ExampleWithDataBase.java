package ru.gnkoshelev.kontur.intern.redis.map.example;

import ru.gnkoshelev.kontur.intern.redis.map.RedisMap;

import java.util.HashMap;

public class ExampleWithDataBase {
    private static DataBase dataBase = new DataBase();

    public static void main(String[] args) {
        System.out.println("DataBase with HashMap");
        dataBase.setProvider(new HashMap<>());

        inputDB(dataBase);

        dataBase.login("Denis", "123");
        dataBase.login("Denis", "111");
        dataBase.login("Andrey", "321");
        dataBase.clear();
        System.out.println("__________");

        System.out.println("DataBase with RedisMap");
        dataBase.setProvider(new RedisMap());

        inputDB(dataBase);
        dataBase.login("Denis", "123");
        dataBase.login("Denis", "111");
        dataBase.login("Andrey", "321");
        dataBase.clear();

    }

    public static void inputDB(DataBase dataBase){
        dataBase.register("Denis", "123");
        dataBase.register("Andrey", "321");
    }
}
