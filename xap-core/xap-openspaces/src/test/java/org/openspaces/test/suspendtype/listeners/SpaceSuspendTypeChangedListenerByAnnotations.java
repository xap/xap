package org.openspaces.test.suspendtype.listeners;

import org.openspaces.core.space.suspend.anntations.SuspendTypeChanged;
import org.openspaces.core.space.suspend.SuspendTypeChangedEvent;

/**
 * @author Elad Gur
 */
public class SpaceSuspendTypeChangedListenerByAnnotations extends AbstractSpaceSuspendTypeChangedListener {

    @SuspendTypeChanged
    public void onSuspendInfoChanged(SuspendTypeChangedEvent event) {
        storeEvent(event);
    }

}
