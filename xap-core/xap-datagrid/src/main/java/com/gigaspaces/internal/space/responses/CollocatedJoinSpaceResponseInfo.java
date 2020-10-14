package com.gigaspaces.internal.space.responses;

import com.gigaspaces.internal.io.IOUtils;
import com.j_spaces.jdbc.query.JoinedQueryResult;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

/**
 * @author yohanakh
 * @since 15.8.0
 */
@com.gigaspaces.api.InternalApi
public class CollocatedJoinSpaceResponseInfo extends AbstractSpaceResponseInfo {
    private static final long serialVersionUID = 1L;
    private JoinedQueryResult result;

    public CollocatedJoinSpaceResponseInfo() {
    }

    public CollocatedJoinSpaceResponseInfo(JoinedQueryResult result) {
        this.result = result;
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        super.writeExternal(out);
        IOUtils.writeObject(out, result);
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        super.readExternal(in);
        result = IOUtils.readObject(in);
    }

    public JoinedQueryResult getResult() {
        return result;
    }

    public void setResult(JoinedQueryResult result) {
        this.result = result;
    }
}
