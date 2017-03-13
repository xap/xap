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

package org.openspaces.spatial.shapes.impl;

import org.locationtech.spatial4j.context.SpatialContext;
import org.locationtech.spatial4j.shape.Rectangle;
import org.locationtech.spatial4j.shape.Shape;

import org.openspaces.spatial.ShapeFormat;
import org.openspaces.spatial.shapes.Point;
import org.openspaces.spatial.shapes.Polygon;
import org.openspaces.spatial.spatial4j.Spatial4jShapeProvider;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.text.ParseException;
import java.util.Arrays;

/**
 * @author Yohana Khoury
 * @since 11.0
 */
public class PolygonImpl implements Polygon, Spatial4jShapeProvider, Externalizable {

    private static final long serialVersionUID = 1L;

    private Point[] points;
    private transient int hashcode;
    private volatile transient org.locationtech.spatial4j.shape.Shape spatial4jShape;

    public PolygonImpl() {
    }

    public PolygonImpl(Point[] points) {
        if (points.length < 3)
            throw new IllegalArgumentException("Polygon requires at least three points");
        if (points.length == 3 && points[0].equals(points[2]))
            throw new IllegalArgumentException("Polygon requires at least three distinct points " + Arrays.asList(points));
        this.points = points;
        initialize();
    }

    private void initialize() {
        if (!points[0].equals(points[points.length - 1])) {
            // Create a copy of points and append the first point at the end:
            Point[] temp = new Point[points.length + 1];
            System.arraycopy(points, 0, temp, 0, points.length);
            temp[points.length] = temp[0];
            points = temp;
        }
        this.hashcode = Arrays.hashCode(points);
    }

    @Override
    public int getNumOfPoints() {
        if (points != null)
            return points.length;
        if (isRectangle(spatial4jShape))
            return 5;
        return getCoordinates(spatial4jShape).length;
    }

    @Override
    public double getX(int index) {
        if (points != null)
            return points[index].getX();
        if (isRectangle(spatial4jShape))
            return getRectangleCoordinate(spatial4jShape, index, 0);
        return getCoordinates(spatial4jShape)[index].getOrdinate(0);
    }

    @Override
    public double getY(int index) {
        if (points != null)
            return points[index].getY();
        if (isRectangle(spatial4jShape))
            return getRectangleCoordinate(spatial4jShape, index, 1);
        return getCoordinates(spatial4jShape)[index].getOrdinate(1);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        PolygonImpl other = (PolygonImpl) o;
        final int length = this.getNumOfPoints();
        if (length != other.getNumOfPoints())
            return false;
        for (int i = 0; i < length; i++) {
            if (this.getX(i) != other.getX(i) || this.getY(i) != other.getY(i))
                return false;
        }
        return true;
    }

    @Override
    public int hashCode() {
        return hashcode;
    }


    @Override
    public String toString() {
        return toString(ShapeFormat.WKT);
    }


    @Override
    public String toString(ShapeFormat shapeFormat) {
        return appendTo(new StringBuilder(), shapeFormat).toString();
    }

    @Override
    public StringBuilder appendTo(StringBuilder stringBuilder, ShapeFormat shapeFormat) {
        switch (shapeFormat) {
            case WKT:
                return appendWkt(stringBuilder);
            case GEOJSON:
                return appendGeoJson(stringBuilder);
            default:
                throw new IllegalArgumentException("Unsupported shape type: " + shapeFormat);
        }
    }

    private StringBuilder appendGeoJson(StringBuilder stringBuilder) {
        stringBuilder.append("{\"type\":\"Polygon\",\"coordinates\":[[");
        appendTuple(stringBuilder, getX(0), getY(0));
        int length = getNumOfPoints();
        for (int i = 1; i < length; i++) {
            stringBuilder.append(',');
            appendTuple(stringBuilder, getX(i), getY(i));
        }
        stringBuilder.append("]]}");
        return stringBuilder;
    }

    private static void appendTuple(StringBuilder stringBuilder, double x, double y) {
        stringBuilder.append('[');
        stringBuilder.append(x);
        stringBuilder.append(',');
        stringBuilder.append(y);
        stringBuilder.append(']');
    }

    private StringBuilder appendWkt(StringBuilder stringBuilder) {
        final int length = getNumOfPoints();
        stringBuilder.append("POLYGON ((");

        for (int i = 0; i < length; i++) {
            if (i != 0)
                stringBuilder.append(", ");
            stringBuilder.append(getX(i));
            stringBuilder.append(' ');
            stringBuilder.append(getY(i));
        }
        stringBuilder.append("))");
        return stringBuilder;
    }

    @Override
    public Shape getSpatial4jShape(SpatialContext spatialContext) {
        org.locationtech.spatial4j.shape.Shape result = this.spatial4jShape;
        if (result == null) {
            try {
                result = spatialContext.getFormats().getWktReader().read(toString(ShapeFormat.WKT));
            } catch (ParseException e) {
                throw new IllegalStateException("Failed to convert polygon to Spatial4J", e);
            } catch (IOException e) {
                throw new IllegalStateException("Failed to convert polygon to Spatial4J", e);
            }

            this.spatial4jShape = result;
            this.points = null;
        }
        return result;
    }

    private static boolean isRectangle(org.locationtech.spatial4j.shape.Shape shape) {
        return shape instanceof org.locationtech.spatial4j.shape.Rectangle;
    }

    private static double getRectangleCoordinate(org.locationtech.spatial4j.shape.Shape shape, int index, int ordinate) {
        org.locationtech.spatial4j.shape.Rectangle rectangle = (Rectangle) shape;
        switch (index) {
            case 0:
                return ordinate == 0 ? rectangle.getMinX() : rectangle.getMinY();
            case 1:
                return ordinate == 0 ? rectangle.getMaxX() : rectangle.getMinY();
            case 2:
                return ordinate == 0 ? rectangle.getMaxX() : rectangle.getMaxY();
            case 3:
                return ordinate == 0 ? rectangle.getMinX() : rectangle.getMaxY();
            case 4:
                return ordinate == 0 ? rectangle.getMinX() : rectangle.getMinY();
            default:
                throw new IllegalArgumentException("Illegal index " + index + " - a rectangle polygon has 4 points");
        }
    }

    private static com.vividsolutions.jts.geom.Coordinate[] getCoordinates(org.locationtech.spatial4j.shape.Shape shape) {
        return ((org.locationtech.spatial4j.shape.jts.JtsGeometry) shape).getGeom().getCoordinates();
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        final int length = getNumOfPoints();
        out.writeInt(length);
        for (int i = 0; i < length; i++) {
            out.writeDouble(getX(i));
            out.writeDouble(getY(i));
        }
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        int length = in.readInt();
        points = new Point[length];
        for (int i = 0; i < length; i++) {
            double x = in.readDouble();
            double y = in.readDouble();
            points[i] = new PointImpl(x, y);
        }
        initialize();
    }
}
