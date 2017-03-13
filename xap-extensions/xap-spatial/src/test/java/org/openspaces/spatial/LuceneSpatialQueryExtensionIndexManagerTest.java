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

package org.openspaces.spatial;

import com.gigaspaces.query.extension.QueryExtensionRuntimeInfo;
import org.locationtech.spatial4j.shape.Shape;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.openspaces.spatial.shapes.Polygon;
import org.openspaces.spatial.spi.LuceneSpatialQueryExtensionManager;
import org.openspaces.spatial.spi.LuceneSpatialQueryExtensionProvider;

import static org.openspaces.spatial.ShapeFactory.point;
import static org.openspaces.spatial.ShapeFactory.polygon;


/**
 * @author Yohana Khoury
 * @since 11.0
 */
public class LuceneSpatialQueryExtensionIndexManagerTest {

    private LuceneSpatialQueryExtensionManager _handler;

    @Rule
    public TemporaryFolder temporaryFolder = new TemporaryFolder();

    @Before
    public void setup() throws Exception {
        QueryExtensionRuntimeInfo config = new QueryExtensionRuntimeInfo() {
            @Override
            public String getSpaceInstanceName() {
                return "dummy";
            }

            @Override
            public String getSpaceInstanceWorkDirectory() {
                return null;
            }
        };
        _handler = new LuceneSpatialQueryExtensionManager(new LuceneSpatialQueryExtensionProvider(), config);
    }

    @Test
    public void testClosedPolygon() throws Exception {

        Polygon polygonWithCloseRing = polygon(point(75.05722045898438, 41.14039880964587),
                point(73.30490112304686, 41.15797827873605),
                point(73.64822387695311, 40.447992135544304),
                point(74.87319946289062, 40.50544628405211),
                point(75.05722045898438, 41.14039880964587));

        Shape spatial4jPolygon = _handler.toShape(polygonWithCloseRing);
        Assert.assertNotNull(spatial4jPolygon);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testIllegalClosedPolygon() throws Exception {

        Polygon polygonWithCloseRing = polygon(point(75.05722045898438, 41.14039880964587),
                point(73.30490112304686, 41.15797827873605),
                point(75.05722045898438, 41.14039880964587));

        Shape spatial4jPolygon = _handler.toShape(polygonWithCloseRing);
        Assert.assertNotNull(spatial4jPolygon);
    }

    @Test
    public void testLegalClosedPolygon() throws Exception {

        Polygon polygonWithCloseRing = polygon(point(75.05722045898438, 41.14039880964587),
                point(73.30490112304686, 41.15797827873605),
                point(73.64822387695311, 40.447992135544304));

        Shape spatial4jPolygon = _handler.toShape(polygonWithCloseRing);
        Assert.assertNotNull(spatial4jPolygon);
    }

    @Test
    public void testConcavePolygon() throws Exception {

        Polygon concavePolygon = polygon(point(5, 5), point(5, 0), point(2.5, 2.5), point(0, 0), point(0, 5));

        Shape spatial4jPolygon = _handler.toShape(concavePolygon);
        Assert.assertNotNull(spatial4jPolygon);
    }

    @Test(expected = org.locationtech.spatial4j.exception.InvalidShapeException.class)
    public void testSelfIntersectionPolygon() throws Exception {

        Polygon concavePolygon = polygon(point(5, 5), point(5, 0), point(2.5, 7.5), point(0, 0), point(0, 5));

        Shape spatial4jPolygon = _handler.toShape(concavePolygon);
        Assert.assertNotNull(spatial4jPolygon);
    }
}