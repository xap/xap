package com.gigaspaces.internal.cluster.node.impl;

import org.junit.Assert;
import org.junit.Test;

public class ReplicationUtilsTest {

    @Test
    public void toShortLookupName() {
        String ipV4Lookup = "mySpace_container1_1:mySpace_LocalView_192.168.34.36[13079]_9dfbb274-15ed-44c2-acde-e1445262c475";
        ReplicationUtils utils = new ReplicationUtils();
        Assert.assertEquals("'toShortLookupName' returned wrong result", "LocalView_9dfbb274", utils.toShortLookupName(ipV4Lookup));

        String ipV6Lookup = "mySpace_container1_1:mySpace_LocalView_[fe80::2fdf:844e:7b3d:b57e][13079]_9dfbb274-15ed-44c2-acde-e1445262c475";
        Assert.assertEquals("'toShortLookupName' returned wrong result", "LocalView_9dfbb274", utils.toShortLookupName(ipV6Lookup));

        String ipV4DurLookup = "replication:NotifyDur:192.168.34.36[22321]_c5fe6896-780f-4df8-a976-8f58d68eddbb";
        Assert.assertEquals("'toShortLookupName' returned wrong result", "NotifyDur_c5fe6896", utils.toShortLookupName(ipV4DurLookup));

        String ipV6DurLookup = "replication:NotifyDur:[fe80::2fdf:844e:7b3d:b57e][22321]_c5fe6896-780f-4df8-a976-8f58d68eddbb";
        Assert.assertEquals("'toShortLookupName' returned wrong result", "NotifyDur_c5fe6896", utils.toShortLookupName(ipV6DurLookup));
    }
}