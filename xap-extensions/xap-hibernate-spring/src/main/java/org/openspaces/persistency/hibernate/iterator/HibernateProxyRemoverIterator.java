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


package org.openspaces.persistency.hibernate.iterator;

import com.gigaspaces.datasource.DataIterator;

import org.openspaces.persistency.support.MultiDataIterator;

/**
 * A wrapper iterator that removes Hibernate proxies from the actual object returned.
 *
 * @author kimchy
 */
public class HibernateProxyRemoverIterator implements MultiDataIterator {

    private DataIterator iterator;

    public HibernateProxyRemoverIterator(DataIterator iterator) {
        this.iterator = iterator;
    }

    public DataIterator[] iterators() {
        if (iterator instanceof MultiDataIterator) {
            DataIterator[] its = ((MultiDataIterator) iterator).iterators();
            DataIterator[] retVal = new DataIterator[its.length];
            for (int i = 0; i < its.length; i++) {
                retVal[i] = new HibernateProxyRemoverIterator(its[i]);
            }
            return retVal;
        }
        return new DataIterator[]{this};
    }

    public void close() {
        iterator.close();
    }

    public boolean hasNext() {
        return iterator.hasNext();
    }

    public Object next() {
        return HibernateIteratorUtils.unproxy(iterator.next());
    }

    public void remove() {
        iterator.remove();
    }
}
