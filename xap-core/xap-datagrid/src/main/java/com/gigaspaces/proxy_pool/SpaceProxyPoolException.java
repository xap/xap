package com.gigaspaces.proxy_pool;


@com.gigaspaces.api.InternalApi
public class SpaceProxyPoolException extends Exception {
    private static final long serialVersionUID = 7716065918777101418L;

    public SpaceProxyPoolException(String msg) {
        super(msg);

    }


    @Override
    public String toString() {
        return "The current session is expired";
    }

}
