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
package com.gigaspaces.internal.server.space.quiesce;

import com.gigaspaces.internal.server.space.SpaceImpl;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import static com.gigaspaces.internal.server.space.quiesce.QuiesceHandler.Status.*;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class QuiesceHandlerTest {

    private QuiesceHandler quiesceHandler;
    @Before
    public void setup() {
        SpaceImpl spaceImplMock = mock(SpaceImpl.class);
        when(spaceImplMock.getServiceName()).thenReturn("mySpace");

        quiesceHandler = new QuiesceHandler(spaceImplMock,null);
    }

    @Test
    public void testAddOneGuard() {
        addGuard(DISCONNECTED);
        assertGuards(DISCONNECTED);
    }

    @Test
    public void testAddTwoGuards() {
        addGuard(QUIESCED);
        addGuard(DISCONNECTED);
        assertGuards(DISCONNECTED, QUIESCED);
    }

    @Test
    public void testAddTwoGuards2() {
        addGuard(DISCONNECTED);
        addGuard(QUIESCED);
        assertGuards(DISCONNECTED, QUIESCED);
    }

    @Test
    public void testAdd1() {
        addGuard(DISCONNECTED);
        addGuard(DEMOTING);
        addGuard(QUIESCED);

        assertAllGuardsExists();
    }

    @Test
    public void testAdd2() {
        addGuard(DISCONNECTED);
        addGuard(QUIESCED);
        addGuard(DEMOTING);

        assertAllGuardsExists();
    }
    @Test
    public void testAdd3() {
        addGuard(DEMOTING);
        addGuard(DISCONNECTED);
        addGuard(QUIESCED);

        assertAllGuardsExists();
    }
    @Test
    public void testAdd4() {
        addGuard(DEMOTING);
        addGuard(QUIESCED);
        addGuard(DISCONNECTED);

        assertAllGuardsExists();
    }
    @Test
    public void testAdd5() {
        addGuard(QUIESCED);
        addGuard(DISCONNECTED);
        addGuard(DEMOTING);

        assertAllGuardsExists();
    }
    @Test
    public void testAdd6() {
        addGuard(QUIESCED);
        addGuard(DEMOTING);
        addGuard(DISCONNECTED);

        assertAllGuardsExists();
    }


    @Test
    public void testFlow1() {
        addGuard(DISCONNECTED);
        addGuard(DEMOTING);
        addGuard(QUIESCED);
        assertAllGuardsExists();

        removeGuard(DISCONNECTED);
        assertGuards(DEMOTING, QUIESCED);

        removeGuard(DEMOTING);
        assertGuards(QUIESCED);

        removeGuard(QUIESCED);
        assertGuards();


        //empty

        addGuard(DISCONNECTED);
        addGuard(DEMOTING);
        addGuard(QUIESCED);


        removeGuard(DISCONNECTED);
        assertGuards(DEMOTING, QUIESCED);

        removeGuard(QUIESCED);
        assertGuards(DEMOTING);

        removeGuard(QUIESCED);
        assertGuards(DEMOTING);

        removeGuard(DEMOTING);
        assertGuards();
    }


    @Test
    public void testDoubleGuard() {
        addGuard(QUIESCED);
        addGuard(QUIESCED);
        assertGuards(QUIESCED);
        removeGuard(QUIESCED);
        assertGuards();

        addGuard(DEMOTING);
        addGuard(DEMOTING);
        assertGuards(DEMOTING);
        removeGuard(DEMOTING);
        assertGuards();
    }

    @Test
    public void testAddDoubleGuardExistingGuards() {
        addGuard(QUIESCED);
        addGuard(QUIESCED);
        assertGuards(QUIESCED);

        addGuard(DEMOTING);
        addGuard(DEMOTING);
        assertGuards(DEMOTING, QUIESCED);

        removeGuard(QUIESCED);
        assertGuards(DEMOTING);

        removeGuard(DEMOTING);
        assertGuards();
    }


    @Test
    public void testDoubleGuardRemove() {
        addGuard(QUIESCED);
        addGuard(DEMOTING);
        addGuard(DISCONNECTED);
        assertAllGuardsExists();

        removeGuard(DEMOTING);
        assertGuards(DISCONNECTED, QUIESCED);
        removeGuard(DEMOTING);
        assertGuards(DISCONNECTED, QUIESCED);

        removeGuard(QUIESCED);
        removeGuard(QUIESCED);
        removeGuard(DISCONNECTED);
        removeGuard(DISCONNECTED);

        assertGuards();

        addGuard(QUIESCED);
        addGuard(DEMOTING);
        addGuard(DISCONNECTED);
        assertAllGuardsExists();
    }

    private void addGuard(QuiesceHandler.Status status) {
        quiesceHandler.addGuard(quiesceHandler.new Guard(status.name(), null, status));
    }


    private void removeGuard(QuiesceHandler.Status status) {
        quiesceHandler.removeGuard(status);
    }


    private void assertGuards(QuiesceHandler.Status ... statuses) {
        QuiesceHandler.Guard current = quiesceHandler.getGuard();
        for (int i = 0 ; i < statuses.length; i++, current = current.getInnerGuard()) {
            Assert.assertNotNull(current);
            Assert.assertEquals(statuses[i], current.getStatus());
        }

        Assert.assertNull(current);
    }

    private void assertAllGuardsExists() {
        assertGuards(DISCONNECTED, DEMOTING, QUIESCED);
    }

}