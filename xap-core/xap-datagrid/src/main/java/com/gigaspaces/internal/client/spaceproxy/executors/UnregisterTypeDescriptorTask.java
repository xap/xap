package com.gigaspaces.internal.client.spaceproxy.executors;

import com.gigaspaces.async.AsyncResult;
import com.gigaspaces.internal.io.IOUtils;
import com.gigaspaces.internal.space.requests.SpaceRequestInfo;
import com.gigaspaces.internal.space.requests.UnregisterTypeDescriptorRequestInfo;
import com.gigaspaces.internal.space.responses.UnregisterTypeDescriptorResponseInfo;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.List;

/**
 * @author Niv Ingberg
 * @since 15.5.1
 */
@com.gigaspaces.api.InternalApi
public class UnregisterTypeDescriptorTask extends SystemDistributedTask<UnregisterTypeDescriptorResponseInfo> {
    private static final long serialVersionUID = 1L;

    private UnregisterTypeDescriptorRequestInfo _actionInfo;

    /**
     * Required for Externalizable
     */
    public UnregisterTypeDescriptorTask() {
    }

    public UnregisterTypeDescriptorTask(UnregisterTypeDescriptorRequestInfo actionInfo) {
        this._actionInfo = actionInfo;
    }

    @Override
    public String toString() {
        return "UnregisterTypeDescriptorTask [" + _actionInfo.typeName + "]";
    }

    @Override
    public SpaceRequestInfo getSpaceRequestInfo() {
        return _actionInfo;
    }

    @Override
    public UnregisterTypeDescriptorResponseInfo reduce(List<AsyncResult<UnregisterTypeDescriptorResponseInfo>> results)
            throws Exception {
        return _actionInfo.reduce(results);
    }

    @Override
    public void writeExternal(ObjectOutput out)
            throws IOException {
        super.writeExternal(out);
        IOUtils.writeObject(out, _actionInfo);
    }

    @Override
    public void readExternal(ObjectInput in)
            throws IOException, ClassNotFoundException {
        super.readExternal(in);
        _actionInfo = IOUtils.readObject(in);
    }
}
