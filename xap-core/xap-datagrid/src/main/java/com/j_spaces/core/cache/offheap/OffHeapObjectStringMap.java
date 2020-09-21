package com.j_spaces.core.cache.offheap;

import com.gigaspaces.offheap.ObjectKey;
import com.gigaspaces.offheap.ObjectValue;
import com.gigaspaces.offheap.OffHeapIndexDriverJNI;

import java.io.*;
import java.util.Collection;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentMap;

public class OffHeapObjectStringMap implements ConcurrentMap<Object, String> {
    private OffHeapIndexDriverJNI offHeapIndexDriverJNI = new OffHeapIndexDriverJNI();
    private long pMap;

    public OffHeapObjectStringMap() {
        this.pMap = offHeapIndexDriverJNI.createMap(100, false);
    }

    @Override
    public int size() {
        return offHeapIndexDriverJNI.size(pMap);
    }

    @Override
    public boolean isEmpty() {
        return size() == 0;
    }

    @Override
    public boolean containsKey(Object key) {
        byte[] keyBytes = getBytesFromObject(key);
        ObjectKey objectKey = new ObjectKey(key.hashCode(), keyBytes, keyBytes.length, "");
        return offHeapIndexDriverJNI.containsKey(pMap, objectKey);
    }

    @Override
    public String get(Object key) {
        byte[] keyBytes = getBytesFromObject(key);
        ObjectKey objectKey = new ObjectKey(key.hashCode(), keyBytes, keyBytes.length, "");
        ObjectValue value = offHeapIndexDriverJNI.get(pMap, objectKey);
        return value == null ? null : (String) getObjectFromBytes(value.data);
    }


    @Override
    public String put(Object key, String value) {
        byte[] keyBytes = getBytesFromObject(key);
        byte[] valueBytes = getBytesFromObject(value);
        ObjectKey objectKey = new ObjectKey(key.hashCode(), keyBytes, keyBytes.length, "");
        ObjectValue objectValue = new ObjectValue(valueBytes, valueBytes.length, "");
        ObjectValue previousValue = offHeapIndexDriverJNI.put(pMap
                , objectKey
                , objectValue);

        return previousValue == null ? null : (String) getObjectFromBytes(previousValue.data);

    }

    @Override
    public String remove(Object key) {
        byte[] keyBytes = getBytesFromObject(key);
        ObjectValue value = offHeapIndexDriverJNI.erase(pMap, new ObjectKey(key.hashCode(), keyBytes, keyBytes.length, ""));
        return value == null ? null : (String) getObjectFromBytes(value.data);
    }

    @Override
    public boolean remove(Object key, Object value) {
        byte[] keyBytes = getBytesFromObject(key);
        byte[] valueBytes = getBytesFromObject(value);
        return offHeapIndexDriverJNI.erase(pMap, new ObjectKey(key.hashCode(), keyBytes, keyBytes.length, ""), new ObjectValue(valueBytes, valueBytes.length, ""));
    }

    @Override
    public String replace(Object key, String value) {
        byte[] keyBytes = getBytesFromObject(key);
        byte[] valueBytes = getBytesFromObject(value);
        ObjectValue result = offHeapIndexDriverJNI.replace(pMap, new ObjectKey(key.hashCode(), keyBytes, keyBytes.length, ""), new ObjectValue(valueBytes, valueBytes.length, ""));
        return result == null ? null : (String) getObjectFromBytes(result.data);

    }

    @Override
    public boolean replace(Object key, String oldValue, String newValue) {
        byte[] keyBytes = getBytesFromObject(key);
        byte[] oldValueBytes = getBytesFromObject(oldValue);
        byte[] newValueBytes = getBytesFromObject(newValue);
        return offHeapIndexDriverJNI.replace(pMap, new ObjectKey(key.hashCode()
                        , keyBytes, keyBytes.length, "")
                , new ObjectValue(oldValueBytes, oldValueBytes.length, "")
                , new ObjectValue(newValueBytes, newValueBytes.length, ""));
    }

    public void freeMap() {
        offHeapIndexDriverJNI.freeMap(pMap);
    }

    @Override
    protected void finalize() throws Throwable {
        freeMap();
    }

    @Override
    public String putIfAbsent(Object key, String value) {
        if (!this.containsKey(key))
            return this.put(key, value);
        else
            return this.get(key);
    }


    @Override
    public boolean containsValue(Object value) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void clear() {
        throw new UnsupportedOperationException();
    }

    @Override
    public void putAll(Map<? extends Object, ? extends String> map) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Set<Object> keySet() {
        throw new UnsupportedOperationException();
    }

    @Override
    public Collection<String> values() {
        throw new UnsupportedOperationException();
    }

    @Override
    public Set<Entry<Object, String>> entrySet() {
        throw new UnsupportedOperationException();
    }

    private byte[] getBytesFromObject(Object object) {
        try (ByteArrayOutputStream bos = new ByteArrayOutputStream()) {
            ObjectOutputStream out = new ObjectOutputStream(bos);
            out.writeObject(object);
            out.flush();
            return bos.toByteArray();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private Object getObjectFromBytes(byte[] bytes) {
        ByteArrayInputStream bis = new ByteArrayInputStream(bytes);
        try (ObjectInput in = new ObjectInputStream(bis)) {
            return in.readObject();

        } catch (IOException e) {
            throw new UncheckedIOException(e);
        } catch (ClassNotFoundException e) {
            throw new IllegalStateException(e);
        }
    }
}
