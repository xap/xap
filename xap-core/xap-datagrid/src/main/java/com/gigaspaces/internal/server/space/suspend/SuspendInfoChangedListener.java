package com.gigaspaces.internal.server.space.suspend;

import com.gigaspaces.server.space.suspend.SuspendInfo;

@com.gigaspaces.api.InternalApi
public interface SuspendInfoChangedListener {

    void onSuspendInfoChanged(SuspendInfo suspendInfo);

}
