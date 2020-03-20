package ru.gnkoshelev.kontur.intern.redis.map.example;

import java.util.Map;

public class DataBase {
    private Map<String, String> provider;

    public void setProvider(Map<String, String> provider) {
        this.provider = provider;
    }

    public void register(String login, String password){
        if(provider.putIfAbsent(login, password) != null){
            System.out.println("login " + login + " already registered");
        }else{
            System.out.println("User " + login + " successfully registered");
        }
    }

    public void login(String login, String password){
        if(provider.get(login).equals(password)){
            System.out.println( login + " logged in ");
        }else{
            System.out.println("Incorrect password or login for " + login);
        }
    }

    public void clear(){
        this.provider.clear();
    }
}
