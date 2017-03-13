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
import org.locationtech.spatial4j.shape.Shape;
import org.locationtech.spatial4j.shape.impl.BufferedLineString;

import org.openspaces.spatial.ShapeFormat;
import org.openspaces.spatial.shapes.LineString;
import org.openspaces.spatial.shapes.Point;
import org.openspaces.spatial.spatial4j.Spatial4jShapeProvider;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * @author Niv Ingberg
 * @since 11.0
 */
public class LineStringImpl implements LineString, Spatial4jShapeProvider, Externalizable {

    private static final long serialVersionUID = 1L;

    private Point[] points;
    private transient int hashcode;
    private volatile transient org.locationtech.spatial4j.shape.Shape spatial4jShape;

    public LineStringImpl() {
    }

    public LineStringImpl(Point[] points) {
        if (points.length < 2)
            throw new IllegalArgumentException("LineString requires at least two points");
        this.points = points;
        initialize();
    }

    private void initialize() {
        this.hashcode = Arrays.hashCode(points);
    }

    @Override
    public int getNumOfPoints() {
        if (points != null)
            return points.length;
        if (isJts())
            return getJtsCoordinates().length;
        return getSpatialPoints().size();
    }

    @Override
    public double getX(int index) {
        if (points != null)
            return points[index].getX();
        if (isJts())
            return getJtsCoordinates()[index].getOrdinate(0);
        return getSpatialPoints().get(index).getX();
    }

    @Override
    public double getY(int index) {
        if (points != null)
            return points[index].getY();
        if (isJts())
            return getJtsCoordinates()[index].getOrdinate(1);
        return getSpatialPoints().get(index).getY();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        LineStringImpl other = (LineStringImpl) o;
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

    private StringBuilder appendWkt(StringBuilder stringBuilder) {
        final int length = getNumOfPoints();
        stringBuilder.append("LINESTRING (");

        for (int i = 0; i < length; i++) {
            if (i != 0)
                stringBuilder.append(", ");
            stringBuilder.append(getX(i));
            stringBuilder.append(' ');
            stringBuilder.append(getY(i));
        }
        stringBuilder.append(")");
        return stringBuilder;
    }

    private StringBuilder appendGeoJson(StringBuilder stringBuilder) {
        stringBuilder.append("{\"type\":\"LineString\",\"coordinates\":[");
        appendTuple(stringBuilder, getX(0), getY(0));
        int length = getNumOfPoints();
        for (int i = 1; i < length; i++) {
            stringBuilder.append(',');
            appendTuple(stringBuilder, getX(i), getY(i));
        }
        stringBuilder.append("]}");
        return stringBuilder;
    }

    private static void appendTuple(StringBuilder stringBuilder, double x, double y) {
        stringBuilder.append('[');
        stringBuilder.append(x);
        stringBuilder.append(',');
        stringBuilder.append(y);
        stringBuilder.append(']');
    }

    @Override
    public Shape getSpatial4jShape(SpatialContext spatialContext) {
        org.locationtech.spatial4j.shape.Shape result = this.spatial4jShape;
        if (result == null) {
            ArrayList<org.locationtech.spatial4j.shape.Point> spatialPoints = new ArrayList<org.locationtech.spatial4j.shape.Point>(points.length);
            for (Point p : points)
                spatialPoints.add(spatialContext.makePoint(p.getX(), p.getY()));
            result = spatialContext.makeLineString(spatialPoints);
            this.spatial4jShape = result;
            this.points = null;
        }
        return result;
    }

    private boolean isJts() {
        return spatial4jShape instanceof org.locationtech.spatial4j.shape.jts.JtsGeometry;
    }

    private com.vividsolutions.jts.geom.Coordinate[] getJtsCoordinates() {
        return ((org.locationtech.spatial4j.shape.jts.JtsGeometry) spatial4jShape).getGeom().getCoordinates();
    }

    private List<org.locationtech.spatial4j.shape.Point> getSpatialPoints() {
        return ((BufferedLineString) spatial4jShape).getPoints();
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
