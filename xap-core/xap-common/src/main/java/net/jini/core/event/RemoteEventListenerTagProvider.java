package net.jini.core.event;

/**
 * Tagging interface on top of {@link RemoteEventListener} that provides additional information
 * (e.g. tag) to be extracted for debugging/logging purposes for a reliable replication target.
 *
 * @since 14.5
 */
public interface RemoteEventListenerTagProvider {

    /**
     * @return A name/id/tag identifying this remote event listener endpoint
     * @since 14.5
     */
    String getTag();
}
