package org.openspaces;

import org.openspaces.core.GigaSpace;
import org.openspaces.core.GigaSpaceConfigurer;
import org.openspaces.core.space.EmbeddedSpaceConfigurer;

import java.util.concurrent.atomic.AtomicInteger;

public class MyMain {
    private static AtomicInteger count = new AtomicInteger(1);
    private static AtomicInteger ageCount = new AtomicInteger(0);


    public static void main(String[] args) {
         GigaSpace space = new GigaSpaceConfigurer(new EmbeddedSpaceConfigurer("demo")).gigaSpace();
//        int numOfEntries = 10_000;
//        for (int i=0 ; i < numOfEntries ; i++) {
//            space.write(new Person(String.valueOf(i)));
//        }
        //dump();

        //        LeaseContext<Person> leaseContext2 = space.write(new Person("1234", "Efrat", 35, "Kimchi"));
//        LeaseContext<Person> leaseContext3 = space.write(new Person("5678", "Amir",33, "Kimchi"));
//        LeaseContext<Person> leaseContext4 = space.write(new Person("3456", "Efrat", 28, "Altman")) ;
        
        System.out.println(space.count(new Person()));


        System.out.println("end");
    }
}
