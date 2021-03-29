/*
 * Copyright (c) 2008-2018, GigaSpaces Technologies, Inc. All Rights Reserved.
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

package org.openspaces.test.suspendtype.listeners;

import com.gigaspaces.admin.quiesce.DefaultQuiesceToken;
import com.gigaspaces.internal.server.space.quiesce.QuiesceHandler;
import com.gigaspaces.server.space.suspend.SuspendType;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.openspaces.core.GigaSpace;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import java.util.LinkedList;
import java.util.List;

@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(locations = {"suspend-info-config.xml"})
public class SpaceStatusChangedAPITest {

    @Autowired
    protected SpaceStatusListenerByAnnotations listenerByAnnotations;

    @Autowired
    protected SpaceStatusChangedListenerByInterface listenerByInterface;

    @Autowired
    protected GigaSpace gigaSpace;
    private List<SuspendType> events;
    private QuiesceHandler quiesceHandler;

    @Before
    public void setup() {
        events = new LinkedList<SuspendType>();
        events.add(SuspendType.NONE);
        quiesceHandler = gigaSpace.getSpace().getDirectProxy().getSpaceImplIfEmbedded().getQuiesceHandler();
    }

    @Test
    public void suspendTypeChangedRegisterTest() {
        // Quiesce Check
        doQuiesce();
        doUnQuiesce();

        Assert.assertEquals("expecting suspend type of NONE after QUIESCE and UN-QUIESCE",getSuspendType(), SuspendType.NONE);
        assertEventReceived();

        // Demote Check
        doDemote();
        registerSuspendTypeChange(SuspendType.NONE);
        doUnDemote();

        Assert.assertEquals("expecting suspend type of NONE after DEMOTE and UN-DEMOTE",getSuspendType(), SuspendType.NONE);
        assertEventReceived();

        // Check masked quiesce
        doDemote();
        doQuiesce();  // this quiesce will be masked because space is currently demoting
        doUnDemote(); // only here the event quiesce event will be received
        doUnQuiesce();

        Assert.assertEquals("expecting suspend type of NONE after DEMOTE, QUIESCE, UN-DEMOTE, UN-QUIESCE",getSuspendType(), SuspendType.NONE);
        assertEventReceived();
    }

    private SuspendType getSuspendType() {
        return quiesceHandler.getSuspendInfo().getSuspendType();
    }

    private void assertEventReceived() {
        try {
            Thread.sleep(100);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        ensureListenersGotEvents(listenerByInterface);
        ensureListenersGotEvents(listenerByAnnotations);
    }

    private void ensureListenersGotEvents(AbstractSpaceStatusChangedListener listener) {
        String errorMsg = listener + " didn't got all notifications";

        Assert.assertEquals(errorMsg, events, listener.getEvents());
    }

    private void doDemote() {
        // stimulate demote
        quiesceHandler.quiesceDemote("demote");
        registerSuspendTypeChange(SuspendType.DEMOTING);
    }

    private void doUnDemote() {
        // stimulate un-demote
        quiesceHandler.unquiesceDemote();
    }

    private void doQuiesce() {
        quiesceHandler.quiesce("quiesce", new DefaultQuiesceToken());
        Assert.assertTrue("failed to quiesce space", quiesceHandler.isQuiesced());
        registerSuspendTypeChange(SuspendType.QUIESCED);
    }

    private void doUnQuiesce() {
        quiesceHandler.unquiesce();
        Assert.assertFalse("failed to un-quiesce space", quiesceHandler.isQuiesced());
        registerSuspendTypeChange(SuspendType.NONE);
    }

    private void registerSuspendTypeChange(SuspendType newSuspendType) {
        events.add(newSuspendType);
    }

}