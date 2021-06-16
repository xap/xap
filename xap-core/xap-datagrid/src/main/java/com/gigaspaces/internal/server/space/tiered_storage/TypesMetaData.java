package com.gigaspaces.internal.server.space.tiered_storage;

import com.gigaspaces.metrics.LongCounter;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

public class TypesMetaData {
    private final Map<String, LongCounter> totalCounterMap = new ConcurrentHashMap<>();
    private final Map<String, LongCounter> ramCounterMap = new ConcurrentHashMap<>();

    public TypesMetaData(){
        String objectClassName = "java.lang.Object";
        totalCounterMap.put(objectClassName,new LongCounter());
        ramCounterMap.put(objectClassName,new LongCounter());
    }

    public Map<String,Integer> getCounterMap() {
        return totalCounterMap.entrySet().stream().collect(Collectors.toMap(Map.Entry::getKey, e -> (int) e.getValue().getCount()));
    }

    public Map<String,Integer> getRamCounterMap() {
        return ramCounterMap.entrySet().stream().collect(Collectors.toMap(Map.Entry::getKey, e -> (int) e.getValue().getCount()));
    }

    private LongCounter getCounterFromCounterMap(String type){
        if(!totalCounterMap.containsKey(type)){
            totalCounterMap.putIfAbsent(type, new LongCounter());
        }
        return totalCounterMap.get(type);
    }

    private LongCounter getRamCounterFromCounterMap(String type){
        if(!ramCounterMap.containsKey(type)){
            ramCounterMap.putIfAbsent(type, new LongCounter());
        }
        return ramCounterMap.get(type);
    }

    public void increaseCounterMap(String type) {
        getCounterFromCounterMap(type).inc();
    }

    public void increaseRamCounterMap(String type) {
        getRamCounterFromCounterMap(type).inc();
    }

    public void decreaseCounterMap(String type) {
        getCounterFromCounterMap(type).dec();
    }

    public void decreaseRamCounterMap(String type) {
        getRamCounterFromCounterMap(type).dec();
    }

    public void setCounterMap(String typeName, int count) {
        getCounterFromCounterMap(typeName).inc(count);
    }
    public void setRamCounterMap(String typeName, int count) {
        getRamCounterFromCounterMap(typeName).inc(count);
    }
}
