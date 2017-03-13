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

import com.gigaspaces.internal.utils.Assert;
import org.locationtech.spatial4j.context.SpatialContext;
import org.locationtech.spatial4j.context.jts.JtsSpatialContext;
import org.locationtech.spatial4j.shape.jts.JtsGeometry;

import org.openspaces.spatial.shapes.Circle;
import org.openspaces.spatial.shapes.LineString;
import org.openspaces.spatial.shapes.Point;
import org.openspaces.spatial.shapes.Polygon;
import org.openspaces.spatial.shapes.Rectangle;
import org.openspaces.spatial.shapes.Shape;
import org.openspaces.spatial.shapes.impl.CircleImpl;
import org.openspaces.spatial.shapes.impl.LineStringImpl;
import org.openspaces.spatial.shapes.impl.PointImpl;
import org.openspaces.spatial.shapes.impl.PolygonImpl;
import org.openspaces.spatial.shapes.impl.RectangleImpl;

import java.io.IOException;
import java.text.ParseException;
import java.util.Collection;
import java.util.List;

/**
 * Factory class for creating spatial shapes.
 *
 * @author Niv Ingberg
 * @since 11.0
 */
public class ShapeFactory {
    /**
     * Private ctor to prevent instantiating this factory class.
     */
    private ShapeFactory() {
    }

    /**
     * Creates a Point instance.
     *
     * @param x The X coordinate, or Longitude in geospatial contexts
     * @param y The Y coordinate, or Latitude in geospatial contexts
     * @return A new Point instance
     */
    public static Point point(double x, double y) {
        return new PointImpl(x, y);
    }

    /**
     * Creates a Circle instance
     *
     * @param center The center of the circle
     * @param radius The radius of the circle
     * @return A new Circle instance
     */
    public static Circle circle(Point center, double radius) {
        return new CircleImpl(center.getX(), center.getY(), radius);
    }

    /**
     * Creates a Rectangle instance
     *
     * @param minX The left edge of the X coordinate
     * @param maxX The right edge of the X coordinate
     * @param minY The bottom edge of the Y coordinate
     * @param maxY The top edge of the Y coordinate
     * @return A new Rectangle instance
     */
    public static Rectangle rectangle(double minX, double maxX, double minY, double maxY) {
        return new RectangleImpl(minX, maxX, minY, maxY);
    }

    /**
     * Creates a LineString instance from the specified points
     *
     * @param first      The first point
     * @param second     The second point
     * @param morePoints The rest of the points
     * @return A new LineString instance
     */
    public static LineString lineString(Point first, Point second, Point... morePoints) {
        Point[] points = new Point[2 + morePoints.length];
        points[0] = Assert.argumentNotNull(first, "first");
        points[1] = Assert.argumentNotNull(second, "second");
        for (int i = 0; i < morePoints.length; i++)
            points[i + 2] = morePoints[i];
        return lineString(points);
    }

    /**
     * Creates a LineString instance from the specified points
     *
     * @param points The LineString points
     * @return A new LineString instance
     */
    public static LineString lineString(Collection<Point> points) {
        return lineString(points.toArray(new Point[points.size()]));
    }

    private static LineString lineString(Point[] points) {
        return new LineStringImpl(points);
    }

    /**
     * Creates a Polygon instance from the specified points
     *
     * @param first      The first point
     * @param second     The second point
     * @param third      The third point
     * @param morePoints The rest of the points
     * @return A new Polygon instance
     */
    public static Polygon polygon(Point first, Point second, Point third, Point... morePoints) {
        Point[] points = new Point[3 + morePoints.length];
        points[0] = Assert.argumentNotNull(first, "first");
        points[1] = Assert.argumentNotNull(second, "second");
        points[2] = Assert.argumentNotNull(third, "third");
        for (int i = 0; i < morePoints.length; i++)
            points[i + 3] = morePoints[i];

        return polygon(points);
    }

    /**
     * Creates a Polygon instance from the specified points
     *
     * @param points The polygon points
     * @return A new Polygon instance
     */
    public static Polygon polygon(Collection<Point> points) {
        return polygon(points.toArray(new Point[points.size()]));
    }

    private static Polygon polygon(Point[] points) {
        return new PolygonImpl(points);
    }

    /**
     * Parses the specified string using the specified shape format
     *
     * @param s           String to parse
     * @param shapeFormat Shape format to use for parsing
     * @return The created shape instance
     */
    public static Shape parse(String s, ShapeFormat shapeFormat) {
        try {
            return fromSpatial4JShape(getReader(shapeFormat).read(s));
        } catch (IOException e) {
            throw new IllegalStateException("Failed to parse shape using " + shapeFormat, e);
        } catch (ParseException e) {
            throw new IllegalArgumentException("Failed to parse shape using " + shapeFormat, e);
        }
    }

    private static org.locationtech.spatial4j.io.ShapeReader getReader(ShapeFormat shapeFormat) {
        org.locationtech.spatial4j.io.ShapeReader result;
        switch (shapeFormat) {
            case WKT:
                result = getDefaultSpatialContext().getFormats().getWktReader();
                break;
            case GEOJSON:
                result = getDefaultSpatialContext().getFormats().getGeoJsonReader();
                break;
            default:
                throw new IllegalArgumentException("Unsupported Shape Format: " + shapeFormat);
        }
        if (result == null)
            throw new IllegalStateException("No Shape reader for format " + shapeFormat);
        return result;
    }

    private static SpatialContext getDefaultSpatialContext() {
        return JtsSpatialContext.GEO;
    }

    private static Shape fromSpatial4JShape(org.locationtech.spatial4j.shape.Shape shape) {
        if (shape instanceof org.locationtech.spatial4j.shape.Point) {
            org.locationtech.spatial4j.shape.Point point = (org.locationtech.spatial4j.shape.Point) shape;
            return point(point.getX(), point.getY());
        }
        if (shape instanceof org.locationtech.spatial4j.shape.Circle) {
            org.locationtech.spatial4j.shape.Circle circle = (org.locationtech.spatial4j.shape.Circle) shape;
            return circle(point(circle.getCenter().getX(), circle.getCenter().getY()), circle.getRadius());
        }
        if (shape instanceof org.locationtech.spatial4j.shape.Rectangle) {
            org.locationtech.spatial4j.shape.Rectangle rectangle = (org.locationtech.spatial4j.shape.Rectangle) shape;
            return rectangle(rectangle.getMinX(), rectangle.getMaxX(), rectangle.getMinY(), rectangle.getMaxY());
        }
        if (shape instanceof org.locationtech.spatial4j.shape.impl.BufferedLineString) {
            org.locationtech.spatial4j.shape.impl.BufferedLineString spatialLineString = (org.locationtech.spatial4j.shape.impl.BufferedLineString) shape;
            List<org.locationtech.spatial4j.shape.Point> spatialPoints = spatialLineString.getPoints();
            Point[] points = new Point[spatialPoints.size()];
            for (int i = 0; i < points.length; i++)
                points[i] = point(spatialPoints.get(i).getX(), spatialPoints.get(i).getY());
            return lineString(points);
        }
        if (shape instanceof org.locationtech.spatial4j.shape.jts.JtsGeometry)
            return fromJtsGeometry((JtsGeometry) shape);
        throw new IllegalArgumentException("Unsupported shape type: " + shape.getClass().getName());
    }

    private static Shape fromJtsGeometry(org.locationtech.spatial4j.shape.jts.JtsGeometry shape) {
        com.vividsolutions.jts.geom.Coordinate[] coordinates = shape.getGeom().getCoordinates();
        Point[] points = new Point[coordinates.length];
        for (int i = 0; i < coordinates.length; i++)
            points[i] = point(coordinates[i].getOrdinate(0), coordinates[i].getOrdinate(1));
        String shapeType = shape.getGeom().getGeometryType();
        if (shapeType.equals("LineString"))
            return lineString(points);
        if (shapeType.equals("Polygon"))
            return polygon(points);
        throw new IllegalArgumentException("Unsupported shape type: " + shapeType);
    }
}
