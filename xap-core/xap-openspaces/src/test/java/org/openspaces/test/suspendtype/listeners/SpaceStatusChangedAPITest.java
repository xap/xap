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
import org.openspaces.events.asyncpolling.AsyncPollingEventContainerServiceMonitors;
import org.openspaces.events.asyncpolling.SimpleAsyncPollingContainerConfigurer;
import org.openspaces.events.asyncpolling.SimpleAsyncPollingEventListenerContainer;
import org.openspaces.events.notify.NotifyEventContainerServiceMonitors;
import org.openspaces.events.notify.SimpleNotifyContainerConfigurer;
import org.openspaces.events.notify.SimpleNotifyEventListenerContainer;
import org.openspaces.events.polling.PollingEventContainerServiceMonitors;
import org.openspaces.events.polling.SimplePollingContainerConfigurer;
import org.openspaces.events.polling.SimplePollingEventListenerContainer;
import org.openspaces.pu.service.ServiceMonitors;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.Lifecycle;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import java.util.ArrayList;
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
    private SimpleNotifyEventListenerContainer simpleNotifyEventListenerContainer;
    private SimplePollingEventListenerContainer simplePollingEventListenerContainer;
    private SimpleAsyncPollingEventListenerContainer simpleAsyncPollingEventListenerContainer;

    final String QUIESCED = "quiesced";
    final String STARTED = "started";

    @Before
    public void setup() {
        events = new LinkedList<SuspendType>();
        events.add(SuspendType.NONE);
        quiesceHandler = gigaSpace.getSpace().getDirectProxy().getSpaceImplIfEmbedded().getQuiesceHandler();
        addNotifyContainers();
    }

    private void addNotifyContainers() {
        simpleNotifyEventListenerContainer = new SimpleNotifyContainerConfigurer(gigaSpace)
                .template(new Object())
                .eventListener((data, gigaSpace, txStatus, source) -> {})
                .notifyContainer();

        simplePollingEventListenerContainer = new SimplePollingContainerConfigurer(gigaSpace)
                .template(new Object())
                .eventListener((data, gigaSpace, txStatus, source) -> {})
                .pollingContainer();

        simpleAsyncPollingEventListenerContainer = new SimpleAsyncPollingContainerConfigurer(gigaSpace)
                .template(new Object())
                .eventListener((data, gigaSpace, txStatus, source) -> {})
                .pollingContainer();
    }

    @Test
    public void suspendTypeChangedRegisterTest() {
        // Disconnect and reconnect Check
        doDisconnect();
        assertStatusChange(QUIESCED);
        doReconnect();
        assertStatusChange(STARTED);
        assertEventReceived();

        // Quiesce Check
        doQuiesce();
        assertStatusChange(QUIESCED);
        doUnQuiesce();
        assertStatusChange(STARTED);

        Assert.assertEquals("expecting suspend type of NONE after QUIESCE and UN-QUIESCE",getSuspendType(), SuspendType.NONE);
        assertEventReceived();

        // Demote Check
        doDemote();
        assertStatusChange(QUIESCED);
        registerSuspendTypeChange(SuspendType.NONE);
        doUnDemote();
        assertStatusChange(STARTED);

        Assert.assertEquals("expecting suspend type of NONE after DEMOTE and UN-DEMOTE",getSuspendType(), SuspendType.NONE);
        assertEventReceived();

        // Check masked quiesce
        doDemote();
        assertStatusChange(QUIESCED);
        doQuiesce();  // this quiesce will be masked because space is currently demoting
        assertStatusChange(QUIESCED);
        doUnDemote(); // only here the event quiesce event will be received
        assertStatusChange(QUIESCED);
        doUnQuiesce();
        assertStatusChange(STARTED);

        Assert.assertEquals("expecting suspend type of NONE after DEMOTE, QUIESCE, UN-DEMOTE, UN-QUIESCE",getSuspendType(), SuspendType.NONE);
        assertEventReceived();
    }

    private void assertStatusChange(String spaceStatus) {
        //notify listener should be running
        boolean running = spaceStatus.equals(STARTED);
        Assert.assertEquals(running, ((Lifecycle) simpleNotifyEventListenerContainer).isRunning());
        //status should change according to operation
        for (ServiceMonitors servicesMonitor : simpleNotifyEventListenerContainer.getServicesMonitors()) {
            String status = ((NotifyEventContainerServiceMonitors) servicesMonitor).getStatus();
            Assert.assertEquals(spaceStatus, status);
        }

        Assert.assertEquals(running, ((Lifecycle) simplePollingEventListenerContainer).isRunning());
        //status should change according to operation
        for (ServiceMonitors servicesMonitor : simplePollingEventListenerContainer.getServicesMonitors()) {
            String status = ((PollingEventContainerServiceMonitors) servicesMonitor).getStatus();
            Assert.assertEquals(spaceStatus, status);
        }

        Assert.assertEquals(running, ((Lifecycle) simpleAsyncPollingEventListenerContainer).isRunning());
        //status should change according to operation
        for (ServiceMonitors servicesMonitor : simpleAsyncPollingEventListenerContainer.getServicesMonitors()) {
            String status = ((AsyncPollingEventContainerServiceMonitors) servicesMonitor).getStatus();
            Assert.assertEquals(spaceStatus, status);
        }
    }

    private void doDisconnect() {
        quiesceHandler.suspend("suspended");
        Assert.assertTrue(quiesceHandler.isSuspended());
        registerSuspendTypeChange(SuspendType.DISCONNECTED);
    }

    private void doReconnect() {
        quiesceHandler.unsuspend();
        Assert.assertFalse(quiesceHandler.isSuspended());
        registerSuspendTypeChange(SuspendType.NONE);
    }

    private SuspendType getSuspendType() {
        return quiesceHandler.getSuspendInfo().getSuspendType();
    }

    private void assertEventReceived() {
        ensureListenersGotEvents(listenerByInterface);
        ensureListenersGotEvents(listenerByAnnotations);
    }

    private void ensureListenersGotEvents(AbstractSpaceStatusChangedListener listener) {
        String errorMsg = listener + " didn't got all notifications";
        //make a copy so that the list doesn't change while asserting
        ArrayList<SuspendType> received = new ArrayList<>(listener.getEvents());
        Assert.assertEquals(errorMsg, events, received);
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