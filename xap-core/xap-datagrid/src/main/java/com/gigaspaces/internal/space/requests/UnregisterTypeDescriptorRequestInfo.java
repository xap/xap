package com.gigaspaces.internal.space.requests;

import com.gigaspaces.async.AsyncResult;
import com.gigaspaces.internal.io.IOUtils;
import com.gigaspaces.internal.space.responses.UnregisterTypeDescriptorResponseInfo;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.List;

/**
 * @author Niv Ingberg
 * @since 15.5.1
 */
public class UnregisterTypeDescriptorRequestInfo extends AbstractSpaceRequestInfo {
    private static final long serialVersionUID = 1L;

    public String typeName;

    /**
     * Required for Externalizable
     */
    public UnregisterTypeDescriptorRequestInfo() {
    }

    public UnregisterTypeDescriptorRequestInfo(String typeName) {
        this.typeName = typeName;
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        super.writeExternal(out);

        IOUtils.writeString(out, typeName);
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        super.readExternal(in);

        this.typeName = IOUtils.readString(in);
    }

    public UnregisterTypeDescriptorResponseInfo reduce(List<AsyncResult<UnregisterTypeDescriptorResponseInfo>> results)
            throws Exception {
        UnregisterTypeDescriptorResponseInfo finalResult = null;

        for (AsyncResult<UnregisterTypeDescriptorResponseInfo> result : results) {
            if (result.getException() != null)
                throw result.getException();
            UnregisterTypeDescriptorResponseInfo responseInfo = result.getResult();
            if (responseInfo.exception != null)
                throw responseInfo.exception;
            if (finalResult == null) {
                finalResult = responseInfo;
            }
        }

        return finalResult;
    }
}
