package com.gigaspaces.internal.space.responses;

import com.gigaspaces.api.InternalApi;
import com.gigaspaces.internal.io.IOUtils;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

/**
 * @author Niv Ingberg
 * @since 15.5.1
 */
@InternalApi
public class UnregisterTypeDescriptorResponseInfo extends AbstractSpaceResponseInfo {
    private static final long serialVersionUID = 1L;

    public Exception exception;

    /**
     * Required for Externalizable
     */
    public UnregisterTypeDescriptorResponseInfo() {
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        super.writeExternal(out);

        IOUtils.writeObject(out, exception);
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        super.readExternal(in);

        this.exception = IOUtils.readObject(in);
    }
}
