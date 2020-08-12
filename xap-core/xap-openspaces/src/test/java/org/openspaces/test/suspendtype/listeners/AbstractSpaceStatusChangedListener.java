package org.openspaces.test.suspendtype.listeners;

import com.gigaspaces.server.space.suspend.SuspendType;
import org.openspaces.core.space.status.SpaceStatusChangedEvent;

import java.util.LinkedList;
import java.util.List;

public abstract class AbstractSpaceStatusChangedListener {

    private List<SuspendType> events;

    public AbstractSpaceStatusChangedListener() {
        events = new LinkedList<SuspendType>();
    }

    protected void storeEvent(SpaceStatusChangedEvent event) {
        SuspendType suspendType = event.getSuspendType();
        events.add(suspendType);
    }

    public List<SuspendType> getEvents() {
        return events;
    }

    public void clearEvents() {
        events.clear();
    }

}