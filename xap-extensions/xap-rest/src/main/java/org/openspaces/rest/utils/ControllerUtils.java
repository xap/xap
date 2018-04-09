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

package org.openspaces.rest.utils;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.gigaspaces.document.SpaceDocument;
import com.gigaspaces.internal.utils.CollectionUtils;
import com.gigaspaces.metadata.SpacePropertyDescriptor;
import com.gigaspaces.metadata.SpaceTypeDescriptor;
import com.j_spaces.core.UnknownTypeException;

import org.openspaces.core.GigaSpace;
import org.openspaces.core.GigaSpaceConfigurer;
import org.openspaces.core.space.UrlSpaceConfigurer;
import org.openspaces.rest.exceptions.RestException;
import org.openspaces.rest.exceptions.TypeNotFoundException;
import org.openspaces.rest.exceptions.UnsupportedTypeException;
import org.springframework.http.converter.HttpMessageNotReadableException;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.Map.Entry;
import java.util.concurrent.atomic.AtomicReference;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * some helper methods to the SpaceApiController class
 *
 * @author rafi
 * @since 8.0
 */
public class ControllerUtils {
    private static final Logger logger = Logger.getLogger(ControllerUtils.class.getName());
    private static final TypeReference<HashMap<String, Object>[]> typeRef = new TypeReference<HashMap<String, Object>[]>() {
    };
    public static final XapConnectionCache xapCache = new XapConnectionCache();
    public static String spaceName;

    public static String lookupLocators;
    public static String lookupGroups;
    private static final Map<String, Class> javaPrimitives = initJavaPrimitives();

    private static Map<String, Class> initJavaPrimitives() {
        Map<String, Class> result = new HashMap<String, Class>();
        //Java objects
        result.put("int32", Integer.class);
        result.put("int64", Long.class);
        result.put("double", Double.class);
        result.put("number", Double.class);
        result.put("float", Float.class);
        result.put("boolean", Boolean.class);
        result.put("string", String.class);
        result.put("datetime", java.util.Date.class);
        result.put("array", java.util.List.class);
        result.put("set", java.util.Set.class);
        result.put("sortedset", java.util.SortedSet.class);
        result.put("object", SpaceDocument.class);
        return result;
    }

    private static final Set<String> allowedFields = CollectionUtils.toUnmodifiableSet(
            "idProperty", "routingProperty", "fixedProperties", "compoundIndex", "fifoSupport", "blobStoreEnabled", "storageType", "supportsOptimisticLocking", "supportsDynamicProperties");


    public static String date_format;
    public static SimpleDateFormat simpleDateFormat;
    public static ObjectMapper mapper;

    public static boolean isAllowedField(String fieldName) {
        return allowedFields.contains(fieldName);
    }

    public static Class getPrimitiveType(String typeName) {
        return javaPrimitives.get(typeName);
    }

    public static SpaceDocument[] createSpaceDocuments(String type, String body, GigaSpace gigaSpace)
            throws TypeNotFoundException {
        HashMap<String, Object>[] propertyMapArr;
        try {
            //if single json object convert it to array
            String data = body;
            if (!body.startsWith("[")) {
                StringBuilder sb = new StringBuilder(body);
                sb.insert(0, "[");
                sb.append("]");
                data = sb.toString();
            }
            //convert to json
            propertyMapArr = mapper.readValue(data, typeRef);
        } catch (Exception e) {
            throw new HttpMessageNotReadableException(e.getMessage(), e.getCause());
        }
        SpaceDocument[] documents = new SpaceDocument[propertyMapArr.length];
        for (int i = 0; i < propertyMapArr.length; i++) {
            Map<String, Object> typeBasedProperties = getTypeBasedProperties(type, propertyMapArr[i], gigaSpace);
            documents[i] = new SpaceDocument(type, typeBasedProperties);
        }
        return documents;
    }

    public static Map<String, Object>[] createPropertiesResult(SpaceDocument[] docs) {
        Map<String, Object>[] result = new HashMap[docs.length];
        for (int i = 0; i < docs.length; i++) {
            result[i] = new HashMap(docs[i].getProperties());
        }
        return result;
    }


    /**
     * @param documentType
     * @param propertyMap
     * @param gigaSpace
     * @return
     * @throws UnknownTypeException
     * @throws TypeNotFoundException
     */
    private static Map<String, Object> getTypeBasedProperties(String documentType, Map<String, Object> propertyMap, GigaSpace gigaSpace) throws TypeNotFoundException {
        SpaceTypeDescriptor spaceTypeDescriptor = gigaSpace.getTypeManager().getTypeDescriptor(documentType);
        if (spaceTypeDescriptor == null) {
            throw new TypeNotFoundException(documentType);
        } else {
            Map<String, Object> buildTypeBasedProperties = buildTypeBasedProperties(propertyMap, spaceTypeDescriptor, gigaSpace);
            return buildTypeBasedProperties;
        }

    }

    @SuppressWarnings("unchecked")
    private static Map<String, Object> buildTypeBasedProperties(
            Map<String, Object> propertyMap,
            SpaceTypeDescriptor spaceTypeDescriptor, GigaSpace gigaSpace) throws TypeNotFoundException {
        HashMap<String, Object> newPropertyMap = new HashMap<String, Object>();
        for (Entry<String, Object> entry : propertyMap.entrySet()) {
            String propKey = entry.getKey();
            Object oldPropValue = entry.getValue();
            SpacePropertyDescriptor propDesc = spaceTypeDescriptor.getFixedProperty(propKey);
            if (propDesc == null) {
                if (logger.isLoggable(Level.WARNING))
                    logger.warning("could not find SpacePropertyDescriptor for " + propKey + ", using String as property type");
                newPropertyMap.put(propKey, oldPropValue);
            }/*else if(propDesc.getType().equals(Object.class)){
                logger.warning("Existing Type of " + propKey + " is Object, using String as property type");
                newPropertyMap.put(propKey, oldPropValue);
            }*/ else {
                Object convertedObj;
                if (oldPropValue instanceof Map) { //SpaceDocument
                    Map obj = (Map) oldPropValue;
                    SpaceDocument sp = new SpaceDocument();
                    sp.setTypeName((String) obj.get("typeName"));
                    if (obj.get("version") != null)
                        sp.setVersion((Integer) obj.get("version"));
                    if (obj.get("transient") != null)
                        sp.setTransient((Boolean) obj.get("transient"));
                    sp.addProperties((Map<String, Object>) obj.get("properties"));
                    convertedObj = sp;

                } else if (oldPropValue instanceof List) {
                    List<Map<String, Object>> oldPropValueList = (List<Map<String, Object>>) oldPropValue;

                    int counter = 0;
                    SpaceDocument[] spaceDocuments = new SpaceDocument[oldPropValueList.size()];
                    for (Map<String, Object> map : oldPropValueList) {

                        SpaceDocument document = new SpaceDocument();
                        document.setTypeName((String) map.get("typeName"));
                        if (map.get("version") != null)
                            document.setVersion((Integer) map.get("version"));
                        if (map.get("transient") != null)
                            document.setTransient((Boolean) map.get("transient"));
                        document.addProperties((Map<String, Object>) map.get("properties"));

                        spaceDocuments[counter++] = document;
                    }
                    convertedObj = spaceDocuments;
                } else {
                    //try {
                    convertedObj = convertPropertyToPrimitiveType(String.valueOf(oldPropValue), propDesc.getType(), propKey);
                    /*} catch (UnsupportedTypeException e) {
                        convertedObj = oldPropValue;
					}*/
                }
                newPropertyMap.put(propKey, convertedObj);
            }
        }
        return newPropertyMap;
    }

    public static Object convertPropertyToPrimitiveType(String object, Class type, String propKey) {
        if (type.equals(Long.class) || type.equals(Long.TYPE))
            return Long.valueOf(object);

        if (type.equals(Boolean.class) || type.equals(Boolean.TYPE))
            return Boolean.valueOf(object);

        if (type.equals(Integer.class) || type.equals(Integer.TYPE))
            return Integer.valueOf(object);

        if (type.equals(Byte.class) || type.equals(Byte.TYPE))
            return Byte.valueOf(object);

        if (type.equals(Short.class) || type.equals(Short.TYPE))
            return Short.valueOf(object);

        if (type.equals(Float.class) || type.equals(Float.TYPE))
            return Float.valueOf(object);

        if (type.equals(Double.class) || type.equals(Double.TYPE))
            return Double.valueOf(object);

        if (type.isEnum())
            return Enum.valueOf(type, object);

        if (type.equals(String.class) || type.equals(Object.class))
            return String.valueOf(object);

        if (type.equals(java.util.Date.class)) {
            try {
                return simpleDateFormat.parse(object);
            } catch (ParseException e) {
                throw new RestException("Unable to parse date [" + object + "]. Make sure it matches the format: " + date_format);
            }
        }

        //unknown type
        throw new UnsupportedTypeException("Non primitive type when converting property [" + propKey + "]:" + type);
    }

    /**
     * Open ended thread safe cache for XAP connections
     *
     * @author DeWayne
     */
    public static class XapConnectionCache {
        private final Logger log = Logger.getLogger("XapConnectionCache");
        private static AtomicReference<XapEndpoint> cache = new AtomicReference<XapEndpoint>();

        public XapConnectionCache() {
        }

        public GigaSpace get() {

            synchronized (cache) {
                log.finest("getting space");

                GigaSpace gs = getSpace();
                if (gs != null) return gs;

                log.finest("lookupgroups: " + lookupGroups);
                log.finest("lookupLocators: " + lookupLocators);
                log.finest("spaceName: " + spaceName);
                String url = "jini://*/*/" + spaceName;


                if ((lookupGroups != null && lookupGroups.length() > 0) || (lookupLocators != null && lookupLocators.length() > 0)) {
                    //If one of them are not null then append '?' char
                    url += "?";

                    boolean lookupGroupsSetted = false;

                    if (lookupGroups != null && lookupGroups.length() > 0) {
                        url += "groups=" + lookupGroups;
                        lookupGroupsSetted = true;
                    }

                    if (lookupLocators != null && lookupLocators.length() > 0) {
                        if (lookupGroupsSetted) {
                            url += "&";
                        }

                        url += "locators=" + lookupLocators;
                    }
                }

                log.info("  connecting to " + url);
                UrlSpaceConfigurer usc = new UrlSpaceConfigurer(url);
                gs = new GigaSpaceConfigurer(usc.space()).gigaSpace();
                cache.set(new XapEndpoint(gs, usc));
                log.finest("  returning space");
                return gs;
            }
        }

        /**
         * Gets a space in the cache.  Doesn't open new connections.
         *
         * @return GigaSpace if successful.  Null otherwise.
         */
        private GigaSpace getSpace() {
            XapEndpoint ep = cache.get();
            if (ep == null) return null;
            return ep.space;
        }
    }

    private static class XapEndpoint {
        public GigaSpace space = null;
        public UrlSpaceConfigurer usc = null;

        public XapEndpoint(GigaSpace space, UrlSpaceConfigurer usc) {
            this.space = space;
            this.usc = usc;
        }

    }

}
