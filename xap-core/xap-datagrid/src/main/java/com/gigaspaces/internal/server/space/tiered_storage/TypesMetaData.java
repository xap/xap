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
        LongCounter counter = totalCounterMap.get(type);
        if(counter == null){
            counter = new LongCounter();
            final LongCounter result = totalCounterMap.putIfAbsent(type, counter);
            if(result != null){
                counter = result;
            }
        }
        return counter;
    }

    private LongCounter getRamCounterFromCounterMap(String type){
        LongCounter counter = ramCounterMap.get(type);
        if(!ramCounterMap.containsKey(type)){
            counter = new LongCounter();
             LongCounter result = ramCounterMap.putIfAbsent(type, counter);
             if(result != null){
                 counter = result;
             }
        }
        return counter;
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
