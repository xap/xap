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
import org.openspaces.spatial.shapes.Circle;
import org.openspaces.spatial.spatial4j.Spatial4jShapeProvider;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

/**
 * @author Barak Bar Orion
 * @since 11.0
 */
public class CircleImpl implements Circle, Spatial4jShapeProvider, Externalizable {

    private static final long serialVersionUID = 1L;

    private double centerX;
    private double centerY;
    private double radius;
    private volatile transient org.locationtech.spatial4j.shape.Shape spatial4jShape;

    public CircleImpl() {
    }

    public CircleImpl(double centerX, double centerY, double radius) {
        this.centerX = centerX;
        this.centerY = centerY;
        this.radius = radius;
    }

    @Override
    public double getCenterX() {
        return centerX;
    }

    @Override
    public double getCenterY() {
        return centerY;
    }

    @Override
    public double getRadius() {
        return radius;
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
        stringBuilder.append("{\"type\":\"Circle\",\"coordinates\":[");
        stringBuilder.append(centerX);
        stringBuilder.append(',');
        stringBuilder.append(centerY);
        stringBuilder.append("],\"radius\":");
        stringBuilder.append(radius);
        stringBuilder.append('}');
        return stringBuilder;
    }

    private StringBuilder appendWkt(StringBuilder stringBuilder) {
        stringBuilder.append("BUFFER (POINT (");
        stringBuilder.append(centerX);
        stringBuilder.append(' ');
        stringBuilder.append(centerY);
        stringBuilder.append("), ");
        stringBuilder.append(radius);
        stringBuilder.append(')');
        return stringBuilder;
    }

    @Override
    public Shape getSpatial4jShape(SpatialContext spatialContext) {
        org.locationtech.spatial4j.shape.Shape result = this.spatial4jShape;
        if (result == null) {
            result = spatialContext.makeCircle(centerX, centerY, radius);
            this.spatial4jShape = result;
        }
        return result;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        Circle other = (Circle) o;
        if (this.centerX != other.getCenterX()) return false;
        if (this.centerY != other.getCenterY()) return false;
        if (this.radius != other.getRadius()) return false;

        return true;
    }

    @Override
    public int hashCode() {
        int result;
        long temp;
        temp = Double.doubleToLongBits(centerX);
        result = (int) (temp ^ (temp >>> 32));
        temp = Double.doubleToLongBits(centerY);
        result = 31 * result + (int) (temp ^ (temp >>> 32));
        temp = Double.doubleToLongBits(radius);
        result = 31 * result + (int) (temp ^ (temp >>> 32));
        return result;
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        out.writeDouble(centerX);
        out.writeDouble(centerY);
        out.writeDouble(radius);
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        centerX = in.readDouble();
        centerY = in.readDouble();
        radius = in.readDouble();
    }
}
