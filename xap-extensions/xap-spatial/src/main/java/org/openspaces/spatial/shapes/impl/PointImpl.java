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
import org.openspaces.spatial.shapes.Point;
import org.openspaces.spatial.spatial4j.Spatial4jShapeProvider;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

/**
 * @author Barak Bar Orion
 * @since 11.0
 */
public class PointImpl implements Point, Spatial4jShapeProvider, Externalizable {
    private static final long serialVersionUID = 1L;

    private double x;
    private double y;
    private volatile transient org.locationtech.spatial4j.shape.Shape spatial4jShape;

    public PointImpl() {
    }

    public PointImpl(double x, double y) {
        this.x = x;
        this.y = y;
    }

    @Override
    public double getX() {
        return x;
    }

    @Override
    public double getY() {
        return y;
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
        stringBuilder.append("POINT (");
        stringBuilder.append(x);
        stringBuilder.append(' ');
        stringBuilder.append(y);
        stringBuilder.append(')');
        return stringBuilder;
    }

    private StringBuilder appendGeoJson(StringBuilder stringBuilder) {
        stringBuilder.append("{\"type\":\"Point\",\"coordinates\":[");
        stringBuilder.append(x);
        stringBuilder.append(',');
        stringBuilder.append(y);
        stringBuilder.append("]}");
        return stringBuilder;
    }

    @Override
    public Shape getSpatial4jShape(SpatialContext spatialContext) {
        org.locationtech.spatial4j.shape.Shape result = this.spatial4jShape;
        if (result == null) {
            result = spatialContext.makePoint(x, y);
            this.spatial4jShape = result;
        }
        return result;
    }

    @Override
    public String toString() {
        return toString(ShapeFormat.WKT);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        Point other = (Point) o;
        if (Double.compare(this.x, other.getX()) != 0) return false;
        if (Double.compare(this.y, other.getY()) != 0) return false;

        return true;
    }

    @Override
    public int hashCode() {
        int result;
        long temp;
        temp = Double.doubleToLongBits(x);
        result = (int) (temp ^ (temp >>> 32));
        temp = Double.doubleToLongBits(y);
        result = 31 * result + (int) (temp ^ (temp >>> 32));
        return result;
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        out.writeDouble(x);
        out.writeDouble(y);
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        x = in.readDouble();
        y = in.readDouble();
    }
}
