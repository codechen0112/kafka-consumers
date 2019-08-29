package com.sqkb.comsumer.addclick;

public class AddClickMain {
    public static void main(String[] args) {
        try {
            String propsName = "adclick.properties";
            new AddClickThread(propsName, 12).start();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
