/*
 * Copyright (c) 2008-2016, GigaSpaces Technologies, Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.gigaspaces.url;

import com.gigaspaces.internal.lookup.SpaceUrlUtils;
import com.gigaspaces.start.SystemInfo;
import com.j_spaces.core.client.SpaceURL;
import com.j_spaces.core.client.SpaceURLParser;

import org.junit.Assert;
import org.junit.Test;

import java.net.MalformedURLException;
import java.util.Properties;

@com.gigaspaces.api.InternalApi
public class SpaceUrlTests {

    //@Test
    public void testDefaults() {
        Assert.assertEquals("/./fooSpace?" + getDefaultSuffixEmbedded(), create("/./fooSpace").getURL());
        Assert.assertEquals("jini://*/*/fooSpace?" + getDefaultSuffixRemote(), create("jini://*/*/fooSpace").getURL());
    }

    //@Test
    public void testAddUpdateRemoveProperties() {
        testUrlPrefix("/./fooSpace");
        testUrlPrefix("jini://*/*/fooSpace");
    }

    private void testUrlPrefix(String urlPrefix) {
        testExplicit(urlPrefix, "securitymanager");
        testExplicit(urlPrefix, "securityManager");
        testExplicit(urlPrefix, "SecurityManager");
        testExplicit(urlPrefix, "SECURITYMANAGER");

        testImplicit(urlPrefix, "uselocalcache");
        testImplicit(urlPrefix, "useLocalCache");
        testImplicit(urlPrefix, "UseLocalCache");
        testImplicit(urlPrefix, "USELOCALCACHE");
    }

    private void testExplicit(String urlPrefix, String name) {
        testExplicitAppend(urlPrefix, name);
        testExplicitAppend(urlPrefix + "?secured", name);
        testExplicitUpdate(urlPrefix, "", name);
        testExplicitUpdate(urlPrefix + "?secured", "", name);
        testExplicitUpdate(urlPrefix, "&mirror", name);
        testExplicitUpdate(urlPrefix + "?secured", "&mirror", name);
    }

    private void testImplicit(String urlPrefix, String name) {
        testImplicitUpdate(urlPrefix, "", name);
        testImplicitUpdate(urlPrefix + "?secured", "", name);
        testImplicitUpdate(urlPrefix, "&mirror", name);
        testImplicitUpdate(urlPrefix + "?secured", "&mirror", name);
    }

    private void testExplicitAppend(String urlPrefix, String name) {
        char separator = urlPrefix.contains("?") ? '&' : '?';
        SpaceURL url = createEmpty(urlPrefix);
        Assert.assertEquals(urlPrefix, url.getURL());
        testAdd(url, name, "foo");
        Assert.assertEquals(urlPrefix + separator + name + '=' + "foo", url.getURL());
        testUpdate(url, name, "bar");
        Assert.assertEquals(urlPrefix + separator + name + '=' + "bar", url.getURL());
        testRemove(url, name);
        Assert.assertEquals(urlPrefix, url.getURL());
    }

    private void testExplicitUpdate(String urlPrefix, String urlSuffix, String name) {
        char separator = urlPrefix.contains("?") ? '&' : '?';
        SpaceURL url = createEmpty(urlPrefix + separator + name + "=foo" + urlSuffix);
        Assert.assertEquals(urlPrefix + separator + name + "=foo" + urlSuffix, url.getURL());
        testUpdate(url, name, "bar");
        Assert.assertEquals(urlPrefix + separator + name + "=bar" + urlSuffix, url.getURL());
        testRemove(url, name);
        if (urlSuffix.length() != 0 && urlSuffix.charAt(0) != separator)
            urlSuffix = separator + urlSuffix.substring(1, urlSuffix.length());
        Assert.assertEquals(urlPrefix + urlSuffix, url.getURL());
    }

    private void testImplicitUpdate(String urlPrefix, String urlSuffix, String name) {
        char separator = urlPrefix.contains("?") ? '&' : '?';
        // Test parse
        SpaceURL url = createEmpty(urlPrefix + separator + name + urlSuffix);
        Assert.assertEquals(urlPrefix + separator + name + urlSuffix, url.getURL());
        Assert.assertTrue(url.containsKey(name));
        Assert.assertEquals("true", url.getProperty(name));
        // Test remove
        url.remove(name);
        String suffix = urlSuffix;
        if (suffix.length() != 0 && suffix.charAt(0) != separator)
            suffix = separator + suffix.substring(1, suffix.length());
        Assert.assertEquals(urlPrefix + suffix, url.getURL());
        Assert.assertFalse(url.containsKey(name));
        Assert.assertNull(url.getProperty(name));
        // Test update
        url = createEmpty(urlPrefix + separator + name + urlSuffix);
        url.setProperty(name, "false");
        Assert.assertEquals(urlPrefix + separator + name + '=' + "false" + urlSuffix, url.getURL());
        Assert.assertTrue(url.containsKey(name));
        Assert.assertEquals("false", url.getProperty(name));
    }

    private void testAdd(SpaceURL url, String name, String value) {
        Assert.assertFalse(url.containsKey(name));
        Assert.assertNull(url.getProperty(name));
        url.setProperty(name, value);
        Assert.assertTrue(url.containsKey(name));
        Assert.assertEquals(value, url.getProperty(name));
    }

    private void testUpdate(SpaceURL url, String name, String value) {
        Assert.assertTrue(url.containsKey(name));
        Assert.assertNotNull(url.getProperty(name));
        url.setProperty(name, value);
        Assert.assertTrue(url.containsKey(name));
        Assert.assertEquals(value, url.getProperty(name));
    }

    private void testRemove(SpaceURL url, String name) {
        Assert.assertTrue(url.containsKey(name));
        Assert.assertNotNull(url.getProperty(name));
        url.remove(name);
        Assert.assertFalse(url.containsKey(name));
        Assert.assertNull(url.getProperty(name));
    }

    //@Test
    public void testRemove() {
        SpaceURL url = create("jini://*/*/fooSpace");
        Assert.assertEquals("jini://*/*/fooSpace?" + buildDefaultGroups() + '&' + buildDefaultState(), url.getURL());
        Assert.assertTrue(url.containsKey(SpaceURL.GROUPS));
        Assert.assertTrue(url.containsKey(SpaceURL.STATE));

        url.remove(SpaceURL.GROUPS);
        Assert.assertEquals("jini://*/*/fooSpace?" + buildDefaultState(), url.getURL());
        Assert.assertFalse(url.containsKey(SpaceURL.GROUPS));
        Assert.assertTrue(url.containsKey(SpaceURL.STATE));

        url.remove(SpaceURL.STATE);
        Assert.assertEquals("jini://*/*/fooSpace", url.getURL());
        Assert.assertFalse(url.containsKey(SpaceURL.GROUPS));
        Assert.assertFalse(url.containsKey(SpaceURL.STATE));
    }

    //@Test
    public void testUseLocalCacheFirstImplicit() {
        SpaceURL url;

        // Test parsing:
        url = create("jini://*/*/fooSpace?useLocalCache");
        Assert.assertEquals("jini://*/*/fooSpace?useLocalCache&" + getDefaultSuffixRemote(), url.getURL());
        Assert.assertEquals(true, url.containsKey(SpaceURL.USE_LOCAL_CACHE));
        Assert.assertEquals("true", url.getProperty(SpaceURL.USE_LOCAL_CACHE));
        // Test disable:
        url.setProperty(SpaceURL.USE_LOCAL_CACHE, "false");
        Assert.assertEquals("jini://*/*/fooSpace?useLocalCache=false&" + getDefaultSuffixRemote(), url.getURL());
        Assert.assertEquals(true, url.containsKey(SpaceURL.USE_LOCAL_CACHE));
        Assert.assertEquals("false", url.getProperty(SpaceURL.USE_LOCAL_CACHE));
        // Test enable:
        url.setProperty(SpaceURL.USE_LOCAL_CACHE, "true");
        Assert.assertEquals("jini://*/*/fooSpace?useLocalCache=true&" + getDefaultSuffixRemote(), url.getURL());
        Assert.assertEquals(true, url.containsKey(SpaceURL.USE_LOCAL_CACHE));
        Assert.assertEquals("true", url.getProperty(SpaceURL.USE_LOCAL_CACHE));
        // Test default:
        url.setProperty(SpaceURL.USE_LOCAL_CACHE, "");
        Assert.assertEquals("jini://*/*/fooSpace?useLocalCache&" + getDefaultSuffixRemote(), url.getURL());
        Assert.assertEquals(true, url.containsKey(SpaceURL.USE_LOCAL_CACHE));
        Assert.assertEquals("", url.getProperty(SpaceURL.USE_LOCAL_CACHE));
        // Test remove:
        url.remove(SpaceURL.USE_LOCAL_CACHE);
        Assert.assertEquals("jini://*/*/fooSpace?" + getDefaultSuffixRemote(), url.getURL());
        Assert.assertEquals(false, url.containsKey(SpaceURL.USE_LOCAL_CACHE));
        Assert.assertEquals(null, url.getProperty(SpaceURL.USE_LOCAL_CACHE));
    }

    //@Test
    public void testUseLocalCacheFirstExplicitTrue() {
        SpaceURL url;

        // Test parsing:
        url = create("jini://*/*/fooSpace?useLocalCache=true");
        Assert.assertEquals("jini://*/*/fooSpace?useLocalCache=true&" + getDefaultSuffixRemote(), url.getURL());
        Assert.assertEquals(true, url.containsKey(SpaceURL.USE_LOCAL_CACHE));
        Assert.assertEquals("true", url.getProperty(SpaceURL.USE_LOCAL_CACHE));
        // Test disable:
        url.setProperty(SpaceURL.USE_LOCAL_CACHE, "false");
        Assert.assertEquals("jini://*/*/fooSpace?useLocalCache=false&" + getDefaultSuffixRemote(), url.getURL());
        Assert.assertEquals(true, url.containsKey(SpaceURL.USE_LOCAL_CACHE));
        Assert.assertEquals("false", url.getProperty(SpaceURL.USE_LOCAL_CACHE));
        // Test enable:
        url.setProperty(SpaceURL.USE_LOCAL_CACHE, "true");
        Assert.assertEquals("jini://*/*/fooSpace?useLocalCache=true&" + getDefaultSuffixRemote(), url.getURL());
        Assert.assertEquals(true, url.containsKey(SpaceURL.USE_LOCAL_CACHE));
        Assert.assertEquals("true", url.getProperty(SpaceURL.USE_LOCAL_CACHE));
        // Test default:
        url.setProperty(SpaceURL.USE_LOCAL_CACHE, "");
        Assert.assertEquals("jini://*/*/fooSpace?useLocalCache&" + getDefaultSuffixRemote(), url.getURL());
        Assert.assertEquals(true, url.containsKey(SpaceURL.USE_LOCAL_CACHE));
        Assert.assertEquals("", url.getProperty(SpaceURL.USE_LOCAL_CACHE));
        // Test remove:
        url.remove(SpaceURL.USE_LOCAL_CACHE);
        Assert.assertEquals("jini://*/*/fooSpace?" + getDefaultSuffixRemote(), url.getURL());
        Assert.assertEquals(false, url.containsKey(SpaceURL.USE_LOCAL_CACHE));
        Assert.assertEquals(null, url.getProperty(SpaceURL.USE_LOCAL_CACHE));
    }

    //@Test
    public void testUseLocalCacheFirstExplicitFalse() {
        SpaceURL url;

        // Test parsing:
        url = create("jini://*/*/fooSpace?useLocalCache=false");
        Assert.assertEquals("jini://*/*/fooSpace?useLocalCache=false&" + getDefaultSuffixRemote(), url.getURL());
        Assert.assertEquals(true, url.containsKey(SpaceURL.USE_LOCAL_CACHE));
        Assert.assertEquals("false", url.getProperty(SpaceURL.USE_LOCAL_CACHE));
        // Test enable:
        url.setProperty(SpaceURL.USE_LOCAL_CACHE, "true");
        Assert.assertEquals("jini://*/*/fooSpace?useLocalCache=true&" + getDefaultSuffixRemote(), url.getURL());
        Assert.assertEquals(true, url.containsKey(SpaceURL.USE_LOCAL_CACHE));
        Assert.assertEquals("true", url.getProperty(SpaceURL.USE_LOCAL_CACHE));
        // Test disable:
        url.setProperty(SpaceURL.USE_LOCAL_CACHE, "false");
        Assert.assertEquals("jini://*/*/fooSpace?useLocalCache=false&" + getDefaultSuffixRemote(), url.getURL());
        Assert.assertEquals(true, url.containsKey(SpaceURL.USE_LOCAL_CACHE));
        Assert.assertEquals("false", url.getProperty(SpaceURL.USE_LOCAL_CACHE));
        // Test default:
        url.setProperty(SpaceURL.USE_LOCAL_CACHE, "");
        Assert.assertEquals("jini://*/*/fooSpace?useLocalCache&" + getDefaultSuffixRemote(), url.getURL());
        Assert.assertEquals(true, url.containsKey(SpaceURL.USE_LOCAL_CACHE));
        Assert.assertEquals("", url.getProperty(SpaceURL.USE_LOCAL_CACHE));
        // Test remove:
        url.remove(SpaceURL.USE_LOCAL_CACHE);
        Assert.assertEquals("jini://*/*/fooSpace?" + getDefaultSuffixRemote(), url.getURL());
        Assert.assertEquals(false, url.containsKey(SpaceURL.USE_LOCAL_CACHE));
        Assert.assertEquals(null, url.getProperty(SpaceURL.USE_LOCAL_CACHE));
    }

    //@Test
    public void testUseLocalCacheSecondImplicit() {
        SpaceURL url;

        // Test parsing:
        url = create("jini://*/*/fooSpace?secured&useLocalCache");
        Assert.assertEquals("jini://*/*/fooSpace?secured&useLocalCache&" + getDefaultSuffixRemote(), url.getURL());
        Assert.assertEquals(true, url.containsKey(SpaceURL.USE_LOCAL_CACHE));
        Assert.assertEquals("true", url.getProperty(SpaceURL.USE_LOCAL_CACHE));
        // Test disable:
        url.setProperty(SpaceURL.USE_LOCAL_CACHE, "false");
        Assert.assertEquals("jini://*/*/fooSpace?secured&useLocalCache=false&" + getDefaultSuffixRemote(), url.getURL());
        Assert.assertEquals(true, url.containsKey(SpaceURL.USE_LOCAL_CACHE));
        Assert.assertEquals("false", url.getProperty(SpaceURL.USE_LOCAL_CACHE));
        // Test enable:
        url.setProperty(SpaceURL.USE_LOCAL_CACHE, "true");
        Assert.assertEquals("jini://*/*/fooSpace?secured&useLocalCache=true&" + getDefaultSuffixRemote(), url.getURL());
        Assert.assertEquals(true, url.containsKey(SpaceURL.USE_LOCAL_CACHE));
        Assert.assertEquals("true", url.getProperty(SpaceURL.USE_LOCAL_CACHE));
        // Test default:
        url.setProperty(SpaceURL.USE_LOCAL_CACHE, "");
        Assert.assertEquals("jini://*/*/fooSpace?secured&useLocalCache&" + getDefaultSuffixRemote(), url.getURL());
        Assert.assertEquals(true, url.containsKey(SpaceURL.USE_LOCAL_CACHE));
        Assert.assertEquals("", url.getProperty(SpaceURL.USE_LOCAL_CACHE));
        // Test remove:
        url.remove(SpaceURL.USE_LOCAL_CACHE);
        Assert.assertEquals("jini://*/*/fooSpace?secured&" + getDefaultSuffixRemote(), url.getURL());
        Assert.assertEquals(false, url.containsKey(SpaceURL.USE_LOCAL_CACHE));
        Assert.assertEquals(null, url.getProperty(SpaceURL.USE_LOCAL_CACHE));
    }

    //@Test
    public void testUseLocalCacheSecondExplicitTrue() {
        SpaceURL url;

        // Test parsing:
        url = create("jini://*/*/fooSpace?secured&useLocalCache=true");
        Assert.assertEquals("jini://*/*/fooSpace?secured&useLocalCache=true&" + getDefaultSuffixRemote(), url.getURL());
        Assert.assertEquals(true, url.containsKey(SpaceURL.USE_LOCAL_CACHE));
        Assert.assertEquals("true", url.getProperty(SpaceURL.USE_LOCAL_CACHE));
        // Test disable:
        url.setProperty(SpaceURL.USE_LOCAL_CACHE, "false");
        Assert.assertEquals("jini://*/*/fooSpace?secured&useLocalCache=false&" + getDefaultSuffixRemote(), url.getURL());
        Assert.assertEquals(true, url.containsKey(SpaceURL.USE_LOCAL_CACHE));
        Assert.assertEquals("false", url.getProperty(SpaceURL.USE_LOCAL_CACHE));
        // Test enable:
        url.setProperty(SpaceURL.USE_LOCAL_CACHE, "true");
        Assert.assertEquals("jini://*/*/fooSpace?secured&useLocalCache=true&" + getDefaultSuffixRemote(), url.getURL());
        Assert.assertEquals(true, url.containsKey(SpaceURL.USE_LOCAL_CACHE));
        Assert.assertEquals("true", url.getProperty(SpaceURL.USE_LOCAL_CACHE));
        // Test default:
        url.setProperty(SpaceURL.USE_LOCAL_CACHE, "");
        Assert.assertEquals("jini://*/*/fooSpace?secured&useLocalCache&" + getDefaultSuffixRemote(), url.getURL());
        Assert.assertEquals(true, url.containsKey(SpaceURL.USE_LOCAL_CACHE));
        Assert.assertEquals("", url.getProperty(SpaceURL.USE_LOCAL_CACHE));
        // Test remove:
        url.remove(SpaceURL.USE_LOCAL_CACHE);
        Assert.assertEquals("jini://*/*/fooSpace?secured&" + getDefaultSuffixRemote(), url.getURL());
        Assert.assertEquals(false, url.containsKey(SpaceURL.USE_LOCAL_CACHE));
        Assert.assertEquals(null, url.getProperty(SpaceURL.USE_LOCAL_CACHE));
    }

    //@Test
    public void testUseLocalCacheSecondExplicitFalse() {
        SpaceURL url;

        // Test parsing:
        url = create("jini://*/*/fooSpace?secured&useLocalCache=false");
        Assert.assertEquals("jini://*/*/fooSpace?secured&useLocalCache=false&" + getDefaultSuffixRemote(), url.getURL());
        Assert.assertEquals(true, url.containsKey(SpaceURL.USE_LOCAL_CACHE));
        Assert.assertEquals("false", url.getProperty(SpaceURL.USE_LOCAL_CACHE));
        // Test enable:
        url.setProperty(SpaceURL.USE_LOCAL_CACHE, "true");
        Assert.assertEquals("jini://*/*/fooSpace?secured&useLocalCache=true&" + getDefaultSuffixRemote(), url.getURL());
        Assert.assertEquals(true, url.containsKey(SpaceURL.USE_LOCAL_CACHE));
        Assert.assertEquals("true", url.getProperty(SpaceURL.USE_LOCAL_CACHE));
        // Test disable:
        url.setProperty(SpaceURL.USE_LOCAL_CACHE, "false");
        Assert.assertEquals("jini://*/*/fooSpace?secured&useLocalCache=false&" + getDefaultSuffixRemote(), url.getURL());
        Assert.assertEquals(true, url.containsKey(SpaceURL.USE_LOCAL_CACHE));
        Assert.assertEquals("false", url.getProperty(SpaceURL.USE_LOCAL_CACHE));
        // Test default:
        url.setProperty(SpaceURL.USE_LOCAL_CACHE, "");
        Assert.assertEquals("jini://*/*/fooSpace?secured&useLocalCache&" + getDefaultSuffixRemote(), url.getURL());
        Assert.assertEquals(true, url.containsKey(SpaceURL.USE_LOCAL_CACHE));
        Assert.assertEquals("", url.getProperty(SpaceURL.USE_LOCAL_CACHE));
        // Test remove:
        url.remove(SpaceURL.USE_LOCAL_CACHE);
        Assert.assertEquals("jini://*/*/fooSpace?secured&" + getDefaultSuffixRemote(), url.getURL());
        Assert.assertEquals(false, url.containsKey(SpaceURL.USE_LOCAL_CACHE));
        Assert.assertEquals(null, url.getProperty(SpaceURL.USE_LOCAL_CACHE));
    }

    //@Test
    public void testUseLocalCacheLastImplicit() {
        SpaceURL url;

        // Test parsing:
        url = create("jini://*/*/fooSpace?" + getDefaultSuffixRemote() + "&useLocalCache");
        Assert.assertEquals("jini://*/*/fooSpace?" + getDefaultSuffixRemote() + "&useLocalCache", url.getURL());
        Assert.assertEquals(true, url.containsKey(SpaceURL.USE_LOCAL_CACHE));
        Assert.assertEquals("true", url.getProperty(SpaceURL.USE_LOCAL_CACHE));
        // Test disable:
        url.setProperty(SpaceURL.USE_LOCAL_CACHE, "false");
        Assert.assertEquals("jini://*/*/fooSpace?" + getDefaultSuffixRemote() + "&useLocalCache=false", url.getURL());
        Assert.assertEquals(true, url.containsKey(SpaceURL.USE_LOCAL_CACHE));
        Assert.assertEquals("false", url.getProperty(SpaceURL.USE_LOCAL_CACHE));
        // Test enable:
        url.setProperty(SpaceURL.USE_LOCAL_CACHE, "true");
        Assert.assertEquals("jini://*/*/fooSpace?" + getDefaultSuffixRemote() + "&useLocalCache=true", url.getURL());
        Assert.assertEquals(true, url.containsKey(SpaceURL.USE_LOCAL_CACHE));
        Assert.assertEquals("true", url.getProperty(SpaceURL.USE_LOCAL_CACHE));
        // Test default:
        url.setProperty(SpaceURL.USE_LOCAL_CACHE, "");
        Assert.assertEquals("jini://*/*/fooSpace?" + getDefaultSuffixRemote() + "&useLocalCache", url.getURL());
        Assert.assertEquals(true, url.containsKey(SpaceURL.USE_LOCAL_CACHE));
        Assert.assertEquals("", url.getProperty(SpaceURL.USE_LOCAL_CACHE));
        // Test remove:
        url.remove(SpaceURL.USE_LOCAL_CACHE);
        Assert.assertEquals("jini://*/*/fooSpace?" + getDefaultSuffixRemote(), url.getURL());
        Assert.assertEquals(false, url.containsKey(SpaceURL.USE_LOCAL_CACHE));
        Assert.assertEquals(null, url.getProperty(SpaceURL.USE_LOCAL_CACHE));
    }

    //@Test
    public void testUseLocalCacheLastExplicitTrue() {
        SpaceURL url;

        // Test parsing:
        url = create("jini://*/*/fooSpace?" + getDefaultSuffixRemote() + "&useLocalCache=true");
        Assert.assertEquals("jini://*/*/fooSpace?" + getDefaultSuffixRemote() + "&useLocalCache=true", url.getURL());
        Assert.assertEquals(true, url.containsKey(SpaceURL.USE_LOCAL_CACHE));
        Assert.assertEquals("true", url.getProperty(SpaceURL.USE_LOCAL_CACHE));
        // Test disable:
        url.setProperty(SpaceURL.USE_LOCAL_CACHE, "false");
        Assert.assertEquals("jini://*/*/fooSpace?" + getDefaultSuffixRemote() + "&useLocalCache=false", url.getURL());
        Assert.assertEquals(true, url.containsKey(SpaceURL.USE_LOCAL_CACHE));
        Assert.assertEquals("false", url.getProperty(SpaceURL.USE_LOCAL_CACHE));
        // Test enable:
        url.setProperty(SpaceURL.USE_LOCAL_CACHE, "true");
        Assert.assertEquals("jini://*/*/fooSpace?" + getDefaultSuffixRemote() + "&useLocalCache=true", url.getURL());
        Assert.assertEquals(true, url.containsKey(SpaceURL.USE_LOCAL_CACHE));
        Assert.assertEquals("true", url.getProperty(SpaceURL.USE_LOCAL_CACHE));
        // Test default:
        url.setProperty(SpaceURL.USE_LOCAL_CACHE, "");
        Assert.assertEquals("jini://*/*/fooSpace?" + getDefaultSuffixRemote() + "&useLocalCache", url.getURL());
        Assert.assertEquals(true, url.containsKey(SpaceURL.USE_LOCAL_CACHE));
        Assert.assertEquals("", url.getProperty(SpaceURL.USE_LOCAL_CACHE));
        // Test remove:
        url.remove(SpaceURL.USE_LOCAL_CACHE);
        Assert.assertEquals("jini://*/*/fooSpace?" + getDefaultSuffixRemote(), url.getURL());
        Assert.assertEquals(false, url.containsKey(SpaceURL.USE_LOCAL_CACHE));
        Assert.assertEquals(null, url.getProperty(SpaceURL.USE_LOCAL_CACHE));
    }

    //@Test
    public void testUseLocalCacheLastExplicitFalse() {
        SpaceURL url;

        // Test parsing:
        url = create("jini://*/*/fooSpace?" + getDefaultSuffixRemote() + "&useLocalCache=false");
        Assert.assertEquals("jini://*/*/fooSpace?" + getDefaultSuffixRemote() + "&useLocalCache=false", url.getURL());
        Assert.assertEquals(true, url.containsKey(SpaceURL.USE_LOCAL_CACHE));
        Assert.assertEquals("false", url.getProperty(SpaceURL.USE_LOCAL_CACHE));
        // Test enable:
        url.setProperty(SpaceURL.USE_LOCAL_CACHE, "true");
        Assert.assertEquals("jini://*/*/fooSpace?" + getDefaultSuffixRemote() + "&useLocalCache=true", url.getURL());
        Assert.assertEquals(true, url.containsKey(SpaceURL.USE_LOCAL_CACHE));
        Assert.assertEquals("true", url.getProperty(SpaceURL.USE_LOCAL_CACHE));
        // Test disable:
        url.setProperty(SpaceURL.USE_LOCAL_CACHE, "false");
        Assert.assertEquals("jini://*/*/fooSpace?" + getDefaultSuffixRemote() + "&useLocalCache=false", url.getURL());
        Assert.assertEquals(true, url.containsKey(SpaceURL.USE_LOCAL_CACHE));
        Assert.assertEquals("false", url.getProperty(SpaceURL.USE_LOCAL_CACHE));
        // Test default:
        url.setProperty(SpaceURL.USE_LOCAL_CACHE, "");
        Assert.assertEquals("jini://*/*/fooSpace?" + getDefaultSuffixRemote() + "&useLocalCache", url.getURL());
        Assert.assertEquals(true, url.containsKey(SpaceURL.USE_LOCAL_CACHE));
        Assert.assertEquals("", url.getProperty(SpaceURL.USE_LOCAL_CACHE));
        // Test remove:
        url.remove(SpaceURL.USE_LOCAL_CACHE);
        Assert.assertEquals("jini://*/*/fooSpace?" + getDefaultSuffixRemote(), url.getURL());
        Assert.assertEquals(false, url.containsKey(SpaceURL.USE_LOCAL_CACHE));
        Assert.assertEquals(null, url.getProperty(SpaceURL.USE_LOCAL_CACHE));
    }

    //@Test
    public void testEmptyLocators() throws MalformedURLException {
        SpaceURL url;

        Properties prop = new Properties();
        prop.put(SpaceUrlUtils.toCustomUrlProperty(SpaceURL.LOCATORS), "");
        url = SpaceURLParser.parseURL("jini://*/*/fooSpace", prop);
        Assert.assertEquals("", url.getProperty(SpaceURL.LOCATORS));

        url = create("jini://*/*/fooSpace?locators=");
        Assert.assertEquals("jini://*/*/fooSpace?locators=&" + getDefaultSuffixRemote(), url.getURL());
        Assert.assertEquals("", url.getProperty(SpaceURL.LOCATORS));
    }

    private static SpaceURL create(String url) {
        try {
            return SpaceURLParser.parseURL(url);
        } catch (MalformedURLException e) {
            throw new IllegalStateException("Failed to create url", e);
        }
    }

    private static SpaceURL createEmpty(String url) {
        SpaceURL result = create(url);
        result.remove(SpaceURL.GROUPS);
        result.remove(SpaceURL.STATE);
        result.remove(SpaceURL.SCHEMA_NAME);
        return result;
    }

    private static String getDefaultSuffixEmbedded() {
        return buildDefaultSchema() + '&' + buildDefaultGroups() + '&' + buildDefaultState();
    }

    private static String getDefaultSuffixRemote() {
        return buildDefaultGroups() + '&' + buildDefaultState();
    }

    private static String buildDefaultSchema() {
        return "schema=default";
    }

    private static String buildDefaultState() {
        return "state=started";
    }

    private static String buildDefaultGroups() {
        return "groups=" + SystemInfo.singleton().lookup().defaultGroups();
    }
}
