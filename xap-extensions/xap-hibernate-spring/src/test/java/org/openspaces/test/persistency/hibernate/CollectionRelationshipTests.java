/*
 * Copyright (c) 2008-2016, GigaSpaces Technologies, Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.openspaces.test.persistency.hibernate;

import org.hibernate.*;
import org.hibernate.cfg.*;
import org.hibernate.query.Query;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.function.Function;

public class CollectionRelationshipTests {

    @Test
    public void testOneToManyEager() {
        SessionFactory sessionFactory = new Configuration()
                .setProperty(Environment.HBM2DDL_AUTO, "create")
                .setProperty(Environment.DIALECT, "org.hibernate.dialect.HSQLDialect")
                .setProperty(Environment.DRIVER, "org.hsqldb.jdbcDriver")
                .setProperty(Environment.URL, "jdbc:hsqldb:mem:test2")
                .setProperty(Environment.USER, "sa")
                .setProperty(Environment.PASS, "")
                .addAnnotatedClass(Child.class)
                .addAnnotatedClass(ParentEager.class)
                .addAnnotatedClass(ParentLazy.class)
                .buildSessionFactory();
        try (Session s = sessionFactory.openSession()) {
            Transaction t = s.beginTransaction();
            s.save(new ParentEager("parent-e", "child-e-1", "child-e-2"));
            s.save(new ParentLazy("parent-l", "child-l-1", "child1-l-2"));
            t.commit();
        }

        // Copying entities with lazy references is impossible because session is already closed.
        try {
            test(sessionFactory, ParentLazy.class, this::usingList);
            Assert.fail("Should have failed");
        } catch (LazyInitializationException e) {
            System.out.println("Intercepted expected exception: " + e.getMessage());
        }

        // Copying entities with eager references is possible because references are pre-fetched during session.
        test(sessionFactory, ParentEager.class, this::usingList);
        // Copying entities with eager references via scroll works up to hibernate 5.3.10 but not afterwards.
        test(sessionFactory, ParentEager.class, this::usingScroll);
    }

    private void test(SessionFactory sessionFactory, Class<? extends Parent> type, Function<Query, List<Parent>> listProvider) {

        Session session = sessionFactory.openSession();
        Query query = session.createQuery("FROM " + type.getName());
        List<Parent> entries = listProvider.apply(query);
        session.close();

        for (Parent p : entries) {
            Assert.assertEquals(2, p.getChildren().size());
        }
    }

    private <T> List<T> usingList(Query query) {
        return query.list();
    }

    private <T> List<T> usingScroll(Query query) {
        List<T> result = new ArrayList<>();
        try (ScrollableResults cursor = query.scroll(ScrollMode.FORWARD_ONLY)) {
            while (cursor.next()) {
                result.add((T) cursor.get(0));
            }
        }
        return result;
    }

    private <T> List<T> usingIterator(Query<T> query) {
        List<T> result = new ArrayList<>();
        Iterator<T> iterator = query.iterate();
        while (iterator.hasNext()) {
            result.add(iterator.next());
        }
        return result;
    }
}
