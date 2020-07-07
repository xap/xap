package com.gigaspaces.start.manager;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.net.InetAddress;
import java.net.UnknownHostException;

public class XapManagerClusterTests {

    @Before
    public void setup() {
        System.clearProperty(XapManagerClusterInfo.SERVERS_PROPERTY);
        for (int i=1 ; i < 10 ; i++)
            System.clearProperty(XapManagerClusterInfo.SERVER_PROPERTY + "." + i);
    }

    @Test
    public void parseSingleHostShort() {
        System.setProperty(XapManagerClusterInfo.SERVERS_PROPERTY, "foo");
        XapManagerClusterInfo cluster = new XapManagerClusterInfo(localhost());
        Assert.assertEquals(1, cluster.size());
        Assert.assertEquals("foo", cluster.getServers().get(0).getHost());
    }

    @Test
    public void parseSingleHostShortNotCurr() {
        System.setProperty(XapManagerClusterInfo.SERVERS_PROPERTY, "foo");
        XapManagerClusterInfo cluster = new XapManagerClusterInfo(localhost());
        Assert.assertEquals(1, cluster.size());
        Assert.assertEquals("foo", cluster.getServers().get(0).getHost());
        Assert.assertNull(cluster.getCurrServer());
    }

    @Test
    public void parseMultipleHostsShort() {
        System.setProperty(XapManagerClusterInfo.SERVERS_PROPERTY, "a,b,c");
        XapManagerClusterInfo cluster = new XapManagerClusterInfo(localhost());
        Assert.assertEquals(3, cluster.size());
        Assert.assertEquals("a", cluster.getServers().get(0).getHost());
        Assert.assertEquals("b", cluster.getServers().get(1).getHost());
        Assert.assertEquals("c", cluster.getServers().get(2).getHost());
    }

    @Test
    public void parseSingleHostFull() {
        System.setProperty(XapManagerClusterInfo.SERVER_PROPERTY + ".1", "foo");
        XapManagerClusterInfo cluster = new XapManagerClusterInfo(localhost());
        Assert.assertEquals(1, cluster.size());
        Assert.assertEquals("foo", cluster.getServers().get(0).getHost());
    }

    @Test
    public void parseMultipleHostsFull() {
        System.setProperty(XapManagerClusterInfo.SERVER_PROPERTY + ".1", "a");
        System.setProperty(XapManagerClusterInfo.SERVER_PROPERTY + ".2", "b");
        System.setProperty(XapManagerClusterInfo.SERVER_PROPERTY + ".3", "c");
        XapManagerClusterInfo cluster = new XapManagerClusterInfo(localhost());
        Assert.assertEquals(3, cluster.size());
        Assert.assertEquals("a", cluster.getServers().get(0).getHost());
        Assert.assertEquals("b", cluster.getServers().get(1).getHost());
        Assert.assertEquals("c", cluster.getServers().get(2).getHost());
    }

    @Test
    public void parseMultipleHostsFullWithProperties() {
        System.setProperty(XapManagerClusterInfo.SERVER_PROPERTY + ".1", "a;lus=foo");
        System.setProperty(XapManagerClusterInfo.SERVER_PROPERTY + ".2", "b;rest=8080");
        System.setProperty(XapManagerClusterInfo.SERVER_PROPERTY + ".3", "c;rest=8081;zookeeper=1:2");
        XapManagerClusterInfo cluster = new XapManagerClusterInfo(localhost());
        Assert.assertEquals(3, cluster.size());
        Assert.assertEquals("a", cluster.getServers().get(0).getHost());
        Assert.assertEquals("foo", cluster.getServers().get(0).getLookupService());
        Assert.assertEquals("b", cluster.getServers().get(1).getHost());
        Assert.assertEquals("8080", cluster.getServers().get(1).getAdminRest());
        Assert.assertEquals("c", cluster.getServers().get(2).getHost());
        Assert.assertEquals("8081", cluster.getServers().get(2).getAdminRest());
        Assert.assertEquals("1:2", cluster.getServers().get(2).getZookeeper());
    }

    @Test
    public void parseMultipleHostsFullWithGap() {
        System.setProperty(XapManagerClusterInfo.SERVER_PROPERTY + ".1", "a");
        System.setProperty(XapManagerClusterInfo.SERVER_PROPERTY + ".3", "c");
        XapManagerClusterInfo cluster = new XapManagerClusterInfo(localhost());
        Assert.assertEquals(1, cluster.size());
        Assert.assertEquals("a", cluster.getServers().get(0).getHost());
    }

    @Test
    public void clusterFromShort() {
        System.setProperty(XapManagerClusterInfo.SERVERS_PROPERTY, "a");
        XapManagerClusterInfo cluster = new XapManagerClusterInfo(localhost());
        Assert.assertEquals(1, cluster.size());
        Assert.assertEquals("a", cluster.getServers().get(0).getHost());
    }

    @Test
    public void clusterFromFull() {
        System.setProperty(XapManagerClusterInfo.SERVER_PROPERTY + ".1", "a");
        XapManagerClusterInfo cluster = new XapManagerClusterInfo(localhost());
        Assert.assertEquals(1, cluster.size());
        Assert.assertEquals("a", cluster.getServers().get(0).getHost());
    }

    @Test
    public void ambiguousCluster() {
        System.setProperty(XapManagerClusterInfo.SERVERS_PROPERTY, "a");
        System.setProperty(XapManagerClusterInfo.SERVER_PROPERTY + ".1", "a");
        try {
            new XapManagerClusterInfo(localhost());
            Assert.fail("Should have failed - ambiguous");
        } catch (Exception e) {
            System.out.println("Intercepted expected exception: " + e.getMessage());
        }
    }

    @Test
    public void validInvalidSizes() {
        XapManagerClusterInfo cluster;

        // Test 1:
        System.setProperty(XapManagerClusterInfo.SERVERS_PROPERTY, "a");
        cluster = new XapManagerClusterInfo(localhost());
        Assert.assertEquals(1, cluster.size());
        Assert.assertEquals("a", cluster.getServers().get(0).getHost());

        // Test 2:
        System.setProperty(XapManagerClusterInfo.SERVERS_PROPERTY, "a,b");
        try {
            cluster = new XapManagerClusterInfo(localhost());
            Assert.fail("Should have failed - unsupported cluster size");
        } catch (UnsupportedOperationException e) {
            System.out.println("Intercepted expected exception: " + e.getMessage());
        }

        // Test 3:
        System.setProperty(XapManagerClusterInfo.SERVERS_PROPERTY, "a,b,c");
        cluster = new XapManagerClusterInfo(localhost());
        Assert.assertEquals(3, cluster.size());
        Assert.assertEquals("a", cluster.getServers().get(0).getHost());
        Assert.assertEquals("b", cluster.getServers().get(1).getHost());
        Assert.assertEquals("c", cluster.getServers().get(2).getHost());

        // Test 4:
        System.setProperty(XapManagerClusterInfo.SERVERS_PROPERTY, "a,b,c,d");
        try {
            cluster = new XapManagerClusterInfo(localhost());
            Assert.fail("Should have failed - unsupported cluster size");
        } catch (UnsupportedOperationException e) {
            System.out.println("Intercepted expected exception: " + e.getMessage());
        }
    }

    private static InetAddress localhost() {
        try {
            return InetAddress.getLocalHost();
        } catch (UnknownHostException e) {
            throw new IllegalStateException("Failed to get local host");
        }
    }
}
