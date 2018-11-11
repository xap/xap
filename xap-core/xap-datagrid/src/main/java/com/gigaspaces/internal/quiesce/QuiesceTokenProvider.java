package com.gigaspaces.internal.quiesce;

import com.gigaspaces.admin.quiesce.QuiesceToken;

/**
 * @author yohanakh
 * @since 14.0.0
 */
@com.gigaspaces.api.InternalApi
public interface QuiesceTokenProvider {
    QuiesceToken getToken();
}
