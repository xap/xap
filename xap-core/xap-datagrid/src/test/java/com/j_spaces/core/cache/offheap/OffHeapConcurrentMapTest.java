package com.j_spaces.core.cache.offheap;

import com.gigaspaces.offheap.ObjectKey;
import com.gigaspaces.offheap.ObjectValue;
import org.junit.Assert;
import org.junit.Test;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

public class OffHeapConcurrentMapTest {

    OffHeapConcurrentMap<Person, Integer> myHashMap = new OffHeapConcurrentMap<>();
    private int nThreads = 4;
    private int objectsPerThread = 10;


    @Test
    public void test() throws ExecutionException, InterruptedException {
        ExecutorService service = Executors.newFixedThreadPool(nThreads);

        List<Future<?>> futureList = new ArrayList<>(nThreads);
        for (int i = 0; i < nThreads; i++) {
            int finalI = i;
            futureList.add(i, service.submit(() -> {
                long threadId = Thread.currentThread().getId();
                for (int j = objectsPerThread * finalI; j < (objectsPerThread * finalI) + objectsPerThread; j++) {
                    Assert.assertNull(myHashMap.put(new Person(j, 20), j));
                }
                System.out.println("thread " + threadId + " finished");
            }));
        }

        for (Future<?> future : futureList) {
            future.get();
        }

        int size = myHashMap.size();
        Assert.assertEquals("map size not as expected", objectsPerThread*nThreads, size);
        System.out.println("map size =  " + size);

        for (int i = 0; i < nThreads; i++) {
            for (int j = objectsPerThread * i; j < (objectsPerThread * i) + objectsPerThread; j++) {
                Person key = new Person(j, 20);
                int val = myHashMap.get(key);
                Assert.assertEquals("value of key "+key+" is not as expected", j, val);
                System.out.println("key = " + key + ", value = " + val);
            }
        }

        for (int i = 0; i < nThreads; i++) {
            for (int j = objectsPerThread * i; j < (objectsPerThread * i) + objectsPerThread; j++) {
                Person key = new Person(j, 20);
                Integer remove = myHashMap.remove(key);
                Assert.assertNotNull("delete key result not as expected" + key, remove);
                System.out.println("deleted key " + key);
            }
        }

        size = myHashMap.size();
        Assert.assertEquals("map size not as expected", 0, size);
        System.out.println("map size =  " + size);

        testRemove1();
        testRemove2();
        testReplace();

        service.shutdownNow();
        myHashMap.freeMap();
    }

    private void testContainsKey() {
        Person myKey = new Person(200,22);
        myHashMap.put(myKey, 500);
        Assert.assertTrue(myHashMap.containsKey(myKey));
        myHashMap.remove(myKey);
        Assert.assertFalse(myHashMap.containsKey(myKey));

    }

    private void testRemove1() {
        Person key = new Person(1, 20);

        Integer put1 = myHashMap.put(key, 100);
        Assert.assertNull("first put returned wrong value", put1);
        System.out.println("first put returned " + put1);

        Integer put2 = myHashMap.put(key, 200);
        Assert.assertEquals("second put returned wrong value", new Integer(100), put2);
        System.out.println("second put returned " + put2);

        Integer res1 = myHashMap.remove(key);
        Assert.assertEquals("first remove returned wrong value", new Integer(200), res1);
        System.out.println("first remove returned: key = " + key + ", value = " + res1);

        Integer res2 = myHashMap.remove(key);
        Assert.assertNull("second remove returned wrong value", res2);
        System.out.println("second remove returned: " + res2);
    }

    private void testRemove2() {
        Person key = new Person(1, 20);

        Integer put1 = myHashMap.put(key, 100);
        Assert.assertNull("first put returned wrong value", put1);
        System.out.println("first put returned " + put1);

        Integer put2 = myHashMap.put(key, 200);
        Assert.assertEquals("second put returned wrong value", new Integer(100), put2);
        System.out.println("second put returned " + put2);

        boolean res1 = myHashMap.remove(key, 100);
        Assert.assertFalse("first remove returned wrong value", res1);
        System.out.println("first remove returned: " + res1);

        boolean res2 = myHashMap.remove(key, 200);
        Assert.assertTrue("second remove returned wrong value", res2);
        System.out.println("second remove returned: " + res2);

        boolean res3 = myHashMap.remove(key, 200);
        Assert.assertFalse("third remove returned wrong value", res3);
        System.out.println("third remove returned: " + res2);
    }


    private void testReplace() {
        Person key = new Person(12, 20);
        myHashMap.put(key, 100);
        boolean replace = myHashMap.replace(key, 0, 20);
        Assert.assertFalse(replace);

        Integer oldVal = myHashMap.replace(key, 200);
        Assert.assertEquals(new Integer(100), oldVal);
        replace = myHashMap.replace(key, 200, 300);
        Assert.assertTrue(replace);
    }

    public class Person implements Externalizable {
        private int id;
        private int age;

        public Person() {
        }

        public Person(int id, int age) {
            this.id = id;
            this.age = age;
        }


        @Override
        public void writeExternal(ObjectOutput out) throws IOException {
            out.writeInt(id);
            out.writeInt(age);
        }

        @Override
        public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
            id = in.readInt();
            age = in.readInt();
        }

        @Override
        public String toString() {
            return "Person{" +
                    "id=" + id +
                    ", age=" + age +
                    '}';
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (!(o instanceof Person)) return false;
            Person person = (Person) o;
            return id == person.id &&
                    age == person.age;
        }

        @Override
        public int hashCode() {
            return Objects.hash(id, age);
        }
    }

}