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

package com.gigaspaces.internal.transport;

import com.gigaspaces.entry.VirtualEntry;
import com.gigaspaces.internal.metadata.AbstractTypeIntrospector;
import com.gigaspaces.internal.metadata.SpacePropertyInfo;
import com.gigaspaces.internal.metadata.SpaceTypeInfo;
import com.gigaspaces.internal.metadata.SpaceTypeInfoRepository;
import com.gigaspaces.metadata.SpaceMetadataException;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Info regarding a projection node for a path property
 *
 * @author yechiel
 * @since 9.7
 */
class PathsProjectionNode {
    private final String[] _tokens;   //dummy
    private volatile SpacePropertyInfo[] _propertyInfo; //for this level
    private final List<PathsProjectionNode> _pathsProjectionSubTree;
    private final boolean _bottom;
    private final String _firstFullPath;
    private final PathProjectionInfo _projectionInfo;
    private final int _pos;

    private static final Logger _logger = LoggerFactory.getLogger(com.gigaspaces.logger.Constants.LOGGER_METADATA_POJO);

    PathsProjectionNode(String property, int pos, List<PathProjectionInfo> paths) {
        _propertyInfo = new SpacePropertyInfo[1];
        String[] tokens = new String[]{property};
        _tokens = tokens;
        _pos = pos;
        _bottom = paths.size() == 1 && (paths.get(0).getDepth() == (pos + 1) || (paths.get(0).getDepth() == (pos + 2) && paths.get(0).getTokens()[pos + 1].isEmpty()));

        _firstFullPath = paths.get(0).getFullPath();
        _projectionInfo = paths.get(0);
        if (isBottom()) {
            _pathsProjectionSubTree = null;
            return;
        }
        boolean isCollection = false;
        //build the subtree
        List<PathsProjectionNode> pathsProjectionSubTree = new LinkedList<PathsProjectionNode>();
        //build + problems identify both  a.b and a.b.c
        HashMap<String, List<PathProjectionInfo>> devidedPaths = new HashMap<String, List<PathProjectionInfo>>();
        for (PathProjectionInfo ppi : paths) {
            if (ppi.getDepth() == pos + 1)
                throw new RuntimeException("invalid projection settings " + ppi.getFullPath() + " other projections exist on same root");
            List<PathProjectionInfo> cur = devidedPaths.get(ppi.getTokens()[pos + 1]);
            if (cur != null) {
                //identify both  a.b and a.b.c
                if (ppi.getDepth() == pos + 2 || cur.get(0).getDepth() == pos + 2)
                    throw new RuntimeException("invalid projection settings both " + ppi.getFullPath() + " and " + cur.get(0).getFullPath());
            } else {
                cur = new ArrayList<PathProjectionInfo>(2);
                devidedPaths.put(ppi.getTokens()[pos + 1], cur);
            }
            cur.add(ppi);
            if (ppi.getCollectionIndicators()[_pos])
                isCollection = true;
        }
        //now create a subtree for each value in the hashmap
        for (Map.Entry<String, List<PathProjectionInfo>> mapEntry : devidedPaths.entrySet()) {
            pathsProjectionSubTree.add(new PathsProjectionNode(mapEntry.getKey(), pos + 1, mapEntry.getValue()));
        }
        _pathsProjectionSubTree = pathsProjectionSubTree;
    }

    String getPropertyName() {
        return _tokens[0];
    }

    SpacePropertyInfo getPropertyInfo() {
        return _propertyInfo != null ? _propertyInfo[0] : null;
    }

    boolean isBottom() {
        return _bottom;
    }

    private boolean isRoot() {
        return _pos == 0;
    }

    public String getFullPath() {
        return _firstFullPath;
    }

    private boolean isCollectionDefined() {
        return _projectionInfo.getCollectionIndicators()[_pos];
    }

    //build an object using original value according to projection
    Object buildProjectedPath(Object value) {
        if (value == null)
            return null;
        Object originalValue = null;

        if (isRoot()) {//the value is the property value.
            originalValue = value;
        } else if (value instanceof Map) {
            originalValue = ((Map) value).get(_projectionInfo.getTokens()[_pos]);
        } else if (value instanceof VirtualEntry) {
            originalValue = ((VirtualEntry) value).getProperty(_projectionInfo.getTokens()[_pos]);
        } else {//get my value, parameter is the parent value
            originalValue = AbstractTypeIntrospector.getNestedValue(value, 0, _tokens, _propertyInfo, getFullPath());
        }


        if (originalValue == null)
            return null;

        //create an empty object for this level
        if (originalValue instanceof Collection) {
            /*
            if (!isCollectionDefined())
			{
		    	if (_logger.isErrorEnabled())
		    		_logger.error("property not declared as collection using [*] notation  [" +  _tokens[_pos] + "]" );
		    	throw new SpaceMetadataException("property not declared as collection using [*] notation  [" +  _tokens[_pos] + "] ");
			}
			*/
            Collection c = (Collection) originalValue;
            Class<? extends Collection> ctype = (Class<? extends Collection>) originalValue.getClass();
            Collection newC = null;
            try {
                newC = ctype.newInstance();
            } catch (InstantiationException e) {
                if (_logger.isErrorEnabled())
                    _logger.error("Failed to create instance of type [" + ctype.getName() + "]: " + e.getMessage(), e);
                throw new SpaceMetadataException("Failed to create instance of type [" + ctype.getName() + "]: " + e.getMessage(), e);
            } catch (IllegalAccessException e) {
                if (_logger.isErrorEnabled())
                    _logger.error("Failed to create instance of type [" + ctype.getName() + "]: " + e.getMessage(), e);
                throw new SpaceMetadataException("Failed to create instance of type [" + ctype.getName() + "]: " + e.getMessage(), e);
            }

            for (Object originalMember : c) {
                if (originalMember == null)
                    continue;

                Object newMember = buildSubObject(originalMember);
                if (newMember != null)
                    newC.add(newMember);
            }
            return newC;
        } else {
            if (isCollectionDefined()) {
                if (_logger.isErrorEnabled())
                    _logger.error("property declared as collection using [*] notation but is not [" + _tokens[_pos] + "]");
                throw new SpaceMetadataException("property declared as collection using [*] notation but is not [" + _tokens[_pos] + "] ");
            }
        }


        return buildSubObject(originalValue);
    }


    private Object buildSubObject(Object originalValue) {
        if (isBottom())
            return originalValue;
        if (originalValue instanceof Collection) {
            Collection c = (Collection) originalValue;
            Class<? extends Collection> ctype = (Class<? extends Collection>) originalValue.getClass();
            Collection newC = null;
            try {
                newC = ctype.newInstance();
            } catch (InstantiationException e) {
                if (_logger.isErrorEnabled())
                    _logger.error("Failed to create instance of type [" + ctype.getName() + "]: " + e.getMessage(), e);
                throw new SpaceMetadataException("Failed to create instance of type [" + ctype.getName() + "]: " + e.getMessage(), e);
            } catch (IllegalAccessException e) {
                if (_logger.isErrorEnabled())
                    _logger.error("Failed to create instance of type [" + ctype.getName() + "]: " + e.getMessage(), e);
                throw new SpaceMetadataException("Failed to create instance of type [" + ctype.getName() + "]: " + e.getMessage(), e);
            }

            for (Object originalMember : c) {
                if (originalMember == null)
                    continue;

                Object newMember = buildSubObject(originalMember);
                if (newMember != null)
                    newC.add(newMember);
            }
            return newC;
        }
        if (originalValue instanceof Map) {
            Map m = (Map) originalValue;
            Class<? extends Map> mtype = (Class<? extends Map>) originalValue.getClass();
            Map newM = null;
            try {
                newM = mtype.newInstance();
            } catch (InstantiationException e) {
                if (_logger.isErrorEnabled())
                    _logger.error("Failed to create instance of type [" + mtype.getName() + "]: " + e.getMessage(), e);
                throw new SpaceMetadataException("Failed to create instance of type [" + mtype.getName() + "]: " + e.getMessage(), e);
            } catch (IllegalAccessException e) {
                if (_logger.isErrorEnabled())
                    _logger.error("Failed to create instance of type [" + mtype.getName() + "]: " + e.getMessage(), e);
                throw new SpaceMetadataException("Failed to create instance of type [" + mtype.getName() + "]: " + e.getMessage(), e);
            }
            for (PathsProjectionNode under : _pathsProjectionSubTree) {
                Object subval = under.buildProjectedPath(originalValue);
                newM.put(under.getPropertyName(), subval);
            }

            return newM;
        }
        if (originalValue instanceof VirtualEntry) {
            VirtualEntry v = (VirtualEntry) originalValue;
            Class<? extends VirtualEntry> vetype = (Class<? extends VirtualEntry>) originalValue.getClass();
            VirtualEntry newVE = null;
            try {
                newVE = vetype.newInstance();
            } catch (InstantiationException e) {
                if (_logger.isErrorEnabled())
                    _logger.error("Failed to create instance of type [" + vetype.getName() + "]: " + e.getMessage(), e);
                throw new SpaceMetadataException("Failed to create instance of type [" + vetype.getName() + "]: " + e.getMessage(), e);
            } catch (IllegalAccessException e) {
                if (_logger.isErrorEnabled())
                    _logger.error("Failed to create instance of type [" + vetype.getName() + "]: " + e.getMessage(), e);
                throw new SpaceMetadataException("Failed to create instance of type [" + vetype.getName() + "]: " + e.getMessage(), e);
            }

            newVE.setTypeName(v.getTypeName());
            newVE.setTransient(v.isTransient());
            newVE.setVersion(v.getVersion());
            for (PathsProjectionNode under : _pathsProjectionSubTree) {
                Object subval = under.buildProjectedPath(originalValue);
                if (subval != null || v.containsProperty(under.getPropertyName()))
                    newVE.setProperty(under.getPropertyName(), subval);
            }

            return newVE;
        }


        Class<? extends Object> type = originalValue.getClass();
        SpaceTypeInfo typeInfo = SpaceTypeInfoRepository.getTypeInfo(type);

        Object newVal = typeInfo.createInstance();
        //iterate over lower levels and get the desired members
        for (PathsProjectionNode under : _pathsProjectionSubTree) {
            Object subval = under.buildProjectedPath(originalValue);
            under.getPropertyInfo().setValue(newVal, subval);
        }
        return newVal;


    }


}
