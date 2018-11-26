package com.j_spaces.core.admin;

import com.gigaspaces.api.InternalApi;
import com.gigaspaces.server.space.suspend.SuspendType;

/**
 * Internal listener interface for receiving events when a space change its suspendType
 *
 * @author Elad Gur
 * @since 14.0
 */
@InternalApi
public interface SuspendTypeChangedInternalListener {

    /** Callback method for receiving the events
     * @param suspendType - the new suspendType of the space */
    void onSuspendTypeChanged(SuspendType suspendType);

}
