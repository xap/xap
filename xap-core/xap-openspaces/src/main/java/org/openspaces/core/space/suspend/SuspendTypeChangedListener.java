package org.openspaces.core.space.suspend;

/**
 * Interface receiving events when a space suspend type changes ({@link com.gigaspaces.server.space.suspend.SuspendType})
 *
 * @author Elad Gur
 * @since 14.0.1
 */
public interface SuspendTypeChangedListener {

    /**
     * @param suspendTypeChangedEvent - an event that contain the {@link com.gigaspaces.server.space.suspend.SuspendType}
     *                                and the {@link com.j_spaces.core.IJSpace}}
     */
    void onSuspendTypeChanged(SuspendTypeChangedEvent suspendTypeChangedEvent);

}