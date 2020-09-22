package com.gigaspaces.internal.server.space.executors;

import com.gigaspaces.internal.server.space.SpaceImpl;
import com.gigaspaces.internal.space.requests.SpaceRequestInfo;
import com.gigaspaces.internal.space.requests.UnregisterTypeDescriptorRequestInfo;
import com.gigaspaces.internal.space.responses.SpaceResponseInfo;
import com.gigaspaces.internal.space.responses.UnregisterTypeDescriptorResponseInfo;
import com.gigaspaces.security.authorities.SpaceAuthority;
import com.j_spaces.core.DropClassException;

/**
 * @author Niv Ingberg
 * @since 15.5.1
 */
public class SpaceUnregisterTypeDescriptorExecutor extends SpaceActionExecutor {
    @Override
    public SpaceResponseInfo execute(SpaceImpl space, SpaceRequestInfo spaceRequestInfo) {
        UnregisterTypeDescriptorRequestInfo requestInfo = (UnregisterTypeDescriptorRequestInfo) spaceRequestInfo;
        UnregisterTypeDescriptorResponseInfo responseInfo = new UnregisterTypeDescriptorResponseInfo();

        try {
            space.getEngine().dropClass(requestInfo.typeName);
        } catch (DropClassException e) {
            responseInfo.exception = e;
        }
        return responseInfo;
    }

    @Override
    public SpaceAuthority.SpacePrivilege getPrivilege() {
        return SpaceAuthority.SpacePrivilege.ALTER;
    }
}
