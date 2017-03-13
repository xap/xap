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

import org.openspaces.spatial.ShapeFormat;
import org.openspaces.spatial.shapes.Rectangle;
import org.openspaces.spatial.spatial4j.Spatial4jShapeProvider;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

/**
 * @author Yohana Khoury
 * @since 11.0
 */
public class RectangleImpl implements Rectangle, Spatial4jShapeProvider, Externalizable {

    private static final long serialVersionUID = 1L;

    private double minX;
    private double maxX;
    private double minY;
    private double maxY;
    private volatile transient org.locationtech.spatial4j.shape.Shape spatial4jShape;

    public RectangleImpl() {
    }

    public RectangleImpl(double minX, double maxX, double minY, double maxY) {
        this.minX = minX;
        this.maxX = maxX;
        this.minY = minY;
        this.maxY = maxY;
    }

    @Override
    public double getMinX() {
        return minX;
    }

    @Override
    public double getMinY() {
        return minY;
    }

    @Override
    public double getMaxX() {
        return maxX;
    }

    @Override
    public double getMaxY() {
        return maxY;
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
        stringBuilder.append("{\"type\":\"Polygon\",\"coordinates\": [[");
        appendTuple(stringBuilder, minX, minY);
        stringBuilder.append(',');
        appendTuple(stringBuilder, minX, maxY);
        stringBuilder.append(',');
        appendTuple(stringBuilder, maxX, maxY);
        stringBuilder.append(',');
        appendTuple(stringBuilder, maxX, minY);
        stringBuilder.append(',');
        appendTuple(stringBuilder, minX, minY);
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
        stringBuilder.append("ENVELOPE (");
        stringBuilder.append(minX);
        stringBuilder.append(", ");
        stringBuilder.append(maxX);
        stringBuilder.append(", ");
        stringBuilder.append(maxY);
        stringBuilder.append(", ");
        stringBuilder.append(minY);
        stringBuilder.append(')');
        return stringBuilder;
    }

    @Override
    public Shape getSpatial4jShape(SpatialContext spatialContext) {
        org.locationtech.spatial4j.shape.Shape result = this.spatial4jShape;
        if (result == null) {
            result = spatialContext.makeRectangle(minX, maxX, minY, maxY);
            this.spatial4jShape = result;
        }
        return result;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        Rectangle other = (Rectangle) o;
        if (this.minX != other.getMinX()) return false;
        if (this.maxX != other.getMaxX()) return false;
        if (this.minY != other.getMinY()) return false;
        if (this.maxY != other.getMaxY()) return false;

        return true;
    }

    @Override
    public int hashCode() {
        int result;
        long temp;
        temp = Double.doubleToLongBits(minX);
        result = (int) (temp ^ (temp >>> 32));
        temp = Double.doubleToLongBits(maxX);
        result = 31 * result + (int) (temp ^ (temp >>> 32));
        temp = Double.doubleToLongBits(minY);
        result = 31 * result + (int) (temp ^ (temp >>> 32));
        temp = Double.doubleToLongBits(maxY);
        result = 31 * result + (int) (temp ^ (temp >>> 32));
        return result;
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        out.writeDouble(minX);
        out.writeDouble(maxX);
        out.writeDouble(minY);
        out.writeDouble(maxY);
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        minX = in.readDouble();
        maxX = in.readDouble();
        minY = in.readDouble();
        maxY = in.readDouble();
    }
}
