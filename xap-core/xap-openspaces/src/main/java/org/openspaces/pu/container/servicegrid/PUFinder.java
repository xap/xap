package org.openspaces.pu.container.servicegrid;

import com.gigaspaces.api.InternalApi;
import com.j_spaces.core.client.FinderException;
import com.j_spaces.core.client.LookupFinder;
import com.j_spaces.core.client.LookupRequest;

/**
 * @author Niv Ingberg
 * @since 14.2
 */
@InternalApi
public class PUFinder {

    public static LookupRequest puLookupRequest() {
        return new LookupRequest(PUServiceBean.class);
    }

    public static LookupRequest puLookupRequest(String name) {
        return puLookupRequest().setServiceName(name);
    }

    public static PUServiceBean find(String name) throws FinderException {
        return find(puLookupRequest(name));
    }

    public static PUServiceBean find(LookupRequest request) throws FinderException {
        return (PUServiceBean) LookupFinder.find(request);
    }
}
