package com.gigaspaces.proxy_pool;


import java.util.HashMap;

public class HashValue {
//    HashMapBusy<SpaceName, ref counter + GConnection>
//            , HashMapIdle<SpaceName, timestamp>

    final private HashMap<String, BusyValue> hashMapBusy;
    final private HashMap<String, Long> hashMapIdle;


    public HashValue() {
        hashMapBusy = new HashMap<>();
        hashMapIdle = new HashMap<>();
    }

    @Override
    public String toString() {
        return "HashValue{" +
                "hashMapBusy=" + hashMapBusy +
                ", hashMapIdle=" + hashMapIdle +
                '}';
    }

    public HashMap<String, BusyValue> getHashMapBusy() {
        return hashMapBusy;
    }

    public HashMap<String, Long> getHashMapIdle() {
        return hashMapIdle;
    }

    public boolean isEmpty (){
        return hashMapBusy.size() == 0 && hashMapIdle.size() == 0;
    }

}
