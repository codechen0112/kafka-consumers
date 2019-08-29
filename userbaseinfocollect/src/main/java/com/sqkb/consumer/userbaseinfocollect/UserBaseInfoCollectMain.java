package com.sqkb.consumer.userbaseinfocollect;
public class UserBaseInfoCollectMain {
    public static void main(String[] args) {
        try {
            String propsName = "userbaseinfocollect.properties";
            new ConsumerThread(propsName, 10).start();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
