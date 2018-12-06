package org.openspaces.core.space.status;

/**
 * Interface receiving events when a space suspend type changes ({@link com.gigaspaces.server.space.suspend.SuspendType})
 *
 * @author Elad Gur
 * @since 14.0.1
 */
public interface SpaceStatusChangedEventListener {

    /**
     * @param event - an event that contain the {@link com.gigaspaces.server.space.suspend.SuspendType}
     *                                and the {@link com.j_spaces.core.IJSpace}}
     */
    void onSpaceStatusChanged(SpaceStatusChangedEvent event);

}