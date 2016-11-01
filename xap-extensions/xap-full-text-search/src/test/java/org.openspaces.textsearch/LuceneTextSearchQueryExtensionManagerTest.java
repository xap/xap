package org.openspaces.textsearch;

import com.gigaspaces.query.extension.QueryExtensionRuntimeInfo;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/**
 * @author Danylo_Hurin.
 */
public class LuceneTextSearchQueryExtensionManagerTest {

    private LuceneTextSearchQueryExtensionManager _manager;

    @Before
    public void setup() throws Exception {
        QueryExtensionRuntimeInfo info = new QueryExtensionRuntimeInfo() {
            @Override
            public String getSpaceInstanceName() {
                return "dummy";
            }

            @Override
            public String getSpaceInstanceWorkDirectory() {
                return null;
            }
        };
        LuceneTextSearchQueryExtensionProvider provider = new LuceneTextSearchQueryExtensionProvider();
        LuceneTextSearchConfiguration configuration = new LuceneTextSearchConfiguration(provider, info);
        _manager = new LuceneTextSearchQueryExtensionManager(provider, info, configuration);
    }

    @Test
    public void testAcceptNullGridValue() throws IllegalArgumentException {
        try {
            _manager.accept("", "", LuceneTextSearchQueryExtensionManager.SEARCH_OPERATION_NAME, null, "");
            Assert.fail("Should throw an exception here");
        } catch (IllegalArgumentException e) {}
    }

    @Test
    public void testAcceptNullQueryValue() throws IllegalArgumentException {
        try {
            _manager.accept("", "", LuceneTextSearchQueryExtensionManager.SEARCH_OPERATION_NAME, "", null);
            Assert.fail("Should throw an exception here");
        } catch (IllegalArgumentException e) {}
    }

    @Test
    public void testAcceptWrongOperation() throws IllegalArgumentException {
        try {
            _manager.accept("", "", "wrong_operation", "", "");
            Assert.fail("Should throw an exception here");
        } catch (IllegalArgumentException e) {}
    }

}
