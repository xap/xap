package org.openspaces.test.suspendtype.listeners;

import com.gigaspaces.server.space.suspend.SuspendType;
import org.openspaces.core.space.suspend.SpaceChangeEvent;

import java.util.LinkedList;
import java.util.List;

public abstract class AbstractSpaceSuspendTypeChangedListener {

    private List<SuspendType> events;

    public AbstractSpaceSuspendTypeChangedListener() {
        events = new LinkedList<SuspendType>();
    }

    protected void storeEvent(SpaceChangeEvent event) {
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