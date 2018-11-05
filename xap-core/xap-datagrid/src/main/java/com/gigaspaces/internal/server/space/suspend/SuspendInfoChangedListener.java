package com.gigaspaces.internal.server.space.suspend;

@com.gigaspaces.api.InternalApi
public interface SuspendInfoChangedListener {

    void onSuspendInfoChanged(SuspendInfo suspendInfo);

}
