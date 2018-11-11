package com.gigaspaces.internal.quiesce;

import com.gigaspaces.admin.quiesce.QuiesceToken;

/**
 * @author yohanakh
 * @since 14.0.0
 */
@com.gigaspaces.api.InternalApi
public class QuiesceTokenProviderImpl implements QuiesceTokenProvider {

    private volatile QuiesceToken token;

    public QuiesceTokenProviderImpl() {
    }

    public QuiesceToken getToken() {
        return token;
    }

    public void setToken(QuiesceToken token) {
        this.token = token;
    }
}
