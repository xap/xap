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

package org.openspaces.rest.space;


import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.gigaspaces.annotation.pojo.FifoSupport;
import com.gigaspaces.client.WriteModifiers;
import com.gigaspaces.document.SpaceDocument;
import com.gigaspaces.metadata.SpaceDocumentSupport;
import com.gigaspaces.metadata.SpacePropertyDescriptor;
import com.gigaspaces.metadata.SpaceTypeDescriptor;
import com.gigaspaces.metadata.SpaceTypeDescriptorBuilder;
import com.gigaspaces.metadata.StorageType;
import com.gigaspaces.metadata.index.SpaceIndexType;
import com.gigaspaces.query.IdQuery;
import com.j_spaces.core.client.SQLQuery;

import net.jini.core.lease.Lease;

import org.jsondoc.core.annotation.Api;
import org.jsondoc.core.annotation.ApiBodyObject;
import org.jsondoc.core.annotation.ApiMethod;
import org.jsondoc.core.annotation.ApiPathParam;
import org.jsondoc.core.annotation.ApiQueryParam;
import org.jsondoc.core.pojo.ApiVerb;
import org.openspaces.core.GigaSpace;
import org.openspaces.core.space.CannotFindSpaceException;
import org.openspaces.rest.exceptions.KeyAlreadyExistException;
import org.openspaces.rest.exceptions.ObjectNotFoundException;
import org.openspaces.rest.exceptions.RestException;
import org.openspaces.rest.exceptions.TypeAlreadyRegisteredException;
import org.openspaces.rest.exceptions.TypeNotFoundException;
import org.openspaces.rest.exceptions.UnsupportedTypeException;
import org.openspaces.rest.utils.ControllerUtils;
import org.openspaces.rest.utils.ErrorMessage;
import org.openspaces.rest.utils.ErrorResponse;
import org.openspaces.rest.utils.ExceptionMessage;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.dao.DataAccessException;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.ResponseStatus;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Spring MVC controller for the RESTful Space API <p/> usage examples: GET:
 * http://localhost:8080/rest/data/Item/_introduce_type?spaceid=customerid <p/>
 * http://localhost:8080/rest/data/Item/1 http://192.168.9.47:8080/rest/data/Item/_criteria?q=data2='common'
 * <p/> Limit result size: http://192.168.9.47:8080/rest/data/Item/_criteria?q=data2='common'&s=10
 * <p/> DELETE: curl -XDELETE http://localhost:8080/rest/data/Item/1 curl -XDELETE
 * http://localhost:8080/rest/data/Item/_criteria?q=id=1 <p/> Limit result size: curl -XDELETE
 * http://localhost:8080/rest/data/Item/_criteria?q=data2='common'&s=5 <p/> POST: curl -XPOST -d
 * '[{"id":"1", "data":"testdata", "data2":"common", "nestedData" : {"nestedKey1":"nestedValue1"}},
 * {"id":"2", "data":"testdata2", "data2":"common", "nestedData" : {"nestedKey2":"nestedValue2"}},
 * {"id":"3", "data":"testdata3", "data2":"common", "nestedData" : {"nestedKey3":"nestedValue3"}}]'
 * http://localhost:8080/rest/data/Item <p/> <p/> The response is a json object: On Sucess: {
 * "status" : "success" } If there is a data: { "status" : "success", "data" : {...} or [{...},
 * {...}] } <p/> On Failure: If Inner error (TypeNotFound/ObjectNotFound) then { "status" : "error",
 * "error": { "message": "some error message" } } If it is a XAP exception: { "status" : "error",
 * "error": { "java.class" : "the exception's class name", "message": "exception.getMessage()" } }
 *
 * @author rafi
 * @since 8.0
 */
@Controller
@Api(name = "Space API", description = "Methods for interacting with space")
public class SpaceAPIController {

    private static final String TYPE_DESCRIPTION = "The type name";

    @Value("${spaceName}")
    public void setSpaceName(String spaceName) {
        ControllerUtils.spaceName = spaceName;
    }

    @Value("${lookupGroups}")
    public void setLookupGroups(String lookupGroups) {
        ControllerUtils.lookupGroups = lookupGroups;
    }

    @Value("${lookupLocators}")
    public void setLookupLocators(String lookupLocators) {
        ControllerUtils.lookupLocators = lookupLocators;
    }

    @Value("${datetime_format}")
    public void setDatetimeFormat(String datetimeFormat) {
        logger.info("Using [" + datetimeFormat + "] as datetime format");
        ControllerUtils.date_format = datetimeFormat;
        ControllerUtils.simpleDateFormat = new SimpleDateFormat(datetimeFormat);
        ControllerUtils.mapper = new ObjectMapper();
        ControllerUtils.mapper.setDateFormat(ControllerUtils.simpleDateFormat);
        ControllerUtils.mapper.configure(SerializationFeature.FAIL_ON_EMPTY_BEANS, false);
    }

    private static final String QUERY_PARAM = "query";
    private static final String MAX_PARAM = "max";
    private static final String SPACEID_PARAM = "spaceid";

    private static int maxReturnValues = Integer.MAX_VALUE;
    private static final Logger logger = Logger.getLogger(SpaceAPIController.class.getName());

    private static Object emptyObject = new Object();

    /**
     * REST GET for introducing type to space
     *
     * @param type type name
     * @return Map<String, Object>
     * @deprecated Use {@link #introduceTypeAdvanced(String, String)} instead
     */
    @Deprecated
    @RequestMapping(value = "/{type}/_introduce_type", method = RequestMethod.GET
            , produces = {MediaType.APPLICATION_JSON_VALUE})
    public
    @ResponseBody
    Map<String, Object> introduceType(
            @PathVariable String type,
            @RequestParam(value = SPACEID_PARAM, defaultValue = "id") String spaceID
    ) {
        if (logger.isLoggable(Level.FINE))
            logger.fine("introducing type: " + type);
        Map<String, Object> result = new Hashtable<String, Object>();
        try {
            GigaSpace gigaSpace = ControllerUtils.xapCache.get();
            SpaceTypeDescriptor typeDescriptor = gigaSpace.getTypeManager().getTypeDescriptor(type);
            if (typeDescriptor != null) {
                throw new TypeAlreadyRegisteredException(type);
            }

            SpaceTypeDescriptor spaceTypeDescriptor = new SpaceTypeDescriptorBuilder(type).idProperty(spaceID)
                    .routingProperty(spaceID).supportsDynamicProperties(true).create();
            gigaSpace.getTypeManager().registerTypeDescriptor(spaceTypeDescriptor);
            result.put("status", "success");
        } catch (IllegalStateException e) {
            throw new RestException(e.getMessage());
        }

        return result;
    }

    public String getString(JsonNode node) {
        return node == null ? null : node.asText();
    }

    public Boolean getBoolean(JsonNode node) {
        return node == null ? null : node.asBoolean();
    }

    @ApiMethod(
            path = "{type}/_introduce_type",
            verb = ApiVerb.PUT
            , description = "Introduces the specified type to the space with the provided description in the body"
            , consumes = {MediaType.APPLICATION_JSON_VALUE}, produces = {MediaType.APPLICATION_JSON_VALUE}

    )
    @RequestMapping(value = "/{type}/_introduce_type", method = RequestMethod.PUT
            , consumes = {MediaType.APPLICATION_JSON_VALUE}, produces = {MediaType.APPLICATION_JSON_VALUE})
    public
    @ResponseBody
    Map<String, Object> introduceTypeAdvanced(
            @PathVariable @ApiPathParam(name = "type", description = TYPE_DESCRIPTION) String type,
            @RequestBody(required = false) @ApiBodyObject String requestBody
    ) {
        if (logger.isLoggable(Level.FINE))
            logger.fine("introducing type: " + type);

        if (requestBody == null) {
            throw new RestException("Request body cannot be empty");
        }

        try {
            ObjectMapper mapper = new ObjectMapper();
            mapper.configure(JsonParser.Feature.ALLOW_COMMENTS, true);
            JsonNode actualObj = mapper.readTree(requestBody);

            //check that the json does not have any "unknown" elements
            Iterator<String> iterator = actualObj.fieldNames();
            while (iterator.hasNext()) {
                String fieldName = iterator.next();
                if (!ControllerUtils.isAllowedField(fieldName)) {
                    throw new RestException("Unknown field: " + fieldName);
                }
            }

            SpaceTypeDescriptorBuilder spaceTypeDescriptor = new SpaceTypeDescriptorBuilder(type);

            JsonNode idProperty = actualObj.get("idProperty");
            if (idProperty == null) {
                throw new RestException("idProperty must be provided");
            } else {
                if (!idProperty.isObject()) {
                    throw new RestException("idProperty value must be object");
                }
                Iterator<String> idPropertyIterator = idProperty.fieldNames();
                while (idPropertyIterator.hasNext()) {
                    String fieldName = idPropertyIterator.next();
                    if (!fieldName.equals("propertyName")
                            && !fieldName.equals("autoGenerated")
                            && !fieldName.equals("indexType")) {
                        throw new RestException("Unknown idProperty field: " + fieldName);
                    }
                }
                String propertyName = getString(idProperty.get("propertyName"));
                if (propertyName != null && !idProperty.get("propertyName").isTextual()) {
                    throw new RestException("idProperty.propertyName must be textual");
                }
                Boolean autoGenerated = getBoolean(idProperty.get("autoGenerated"));
                if (autoGenerated != null && !idProperty.get("autoGenerated").isBoolean()) {
                    throw new RestException("idProperty.autoGenerated must be boolean");
                }
                String indexType = getString(idProperty.get("indexType"));
                if (indexType != null && !idProperty.get("indexType").isTextual()) {
                    throw new RestException("idProperty.indexType must be textual");
                }

                if (propertyName == null) {
                    throw new RestException("idProperty.propertyName must be provided");
                } else if (autoGenerated == null && indexType == null) {
                    spaceTypeDescriptor.idProperty(propertyName);
                } else if (autoGenerated != null && indexType == null) {
                    spaceTypeDescriptor.idProperty(propertyName, autoGenerated);
                } else if (autoGenerated != null && indexType != null) {
                    SpaceIndexType spaceIndexType;
                    try {
                        spaceIndexType = SpaceIndexType.valueOf(indexType);
                    } catch (IllegalArgumentException e) {
                        throw new RestException("Illegal idProperty.indexType: " + e.getMessage());
                    }
                    spaceTypeDescriptor.idProperty(propertyName, autoGenerated, spaceIndexType);
                } else {
                    throw new RestException("idProperty.indexType cannot be used without idProperty.autoGenerated");
                }
            }

            JsonNode routingProperty = actualObj.get("routingProperty");
            if (routingProperty != null) {
                if (!routingProperty.isObject()) {
                    throw new RestException("routingProperty value must be object");
                }
                Iterator<String> routingPropertyIterator = routingProperty.fieldNames();
                while (routingPropertyIterator.hasNext()) {
                    String fieldName = routingPropertyIterator.next();
                    if (!fieldName.equals("propertyName") && !fieldName.equals("indexType")) {
                        throw new RestException("Unknown routingProperty field: " + fieldName);
                    }
                }
                String propertyName = getString(routingProperty.get("propertyName"));
                if (propertyName != null && !routingProperty.get("propertyName").isTextual()) {
                    throw new RestException("routingProperty.propertyName must be textual");
                }
                String indexType = getString(routingProperty.get("indexType"));
                if (indexType != null && !routingProperty.get("indexType").isTextual()) {
                    throw new RestException("routingProperty.indexType must be textual");
                }

                if (propertyName == null) {
                    throw new RestException("routingProperty.propertyName must be provided");
                } else if (indexType == null) {
                    spaceTypeDescriptor.routingProperty(propertyName);
                } else { //(indexType != null)
                    SpaceIndexType spaceIndexType;
                    try {
                        spaceIndexType = SpaceIndexType.valueOf(indexType);
                    } catch (IllegalArgumentException e) {
                        throw new RestException("Illegal routingProperty.indexType: " + e.getMessage());
                    }
                    spaceTypeDescriptor.routingProperty(propertyName, spaceIndexType);
                }
            }

            JsonNode compoundIndex = actualObj.get("compoundIndex");
            if (compoundIndex != null) {
                Iterator<String> compoundIndexIterator = compoundIndex.fieldNames();
                while (compoundIndexIterator.hasNext()) {
                    String fieldName = compoundIndexIterator.next();
                    if (!fieldName.equals("paths") && !fieldName.equals("unique")) {
                        throw new RestException("Unknown compoundIndex field: " + fieldName);
                    }
                }
                JsonNode paths = compoundIndex.get("paths");
                if (paths != null && !paths.isArray()) {
                    throw new RestException("compoundIndex.paths must be array of strings");
                }
                Boolean unique = getBoolean(compoundIndex.get("unique"));
                if (unique != null && !compoundIndex.get("unique").isBoolean()) {
                    throw new RestException("compoundIndex.unique must be boolean");
                }

                if (paths == null) {
                    throw new RestException("compoundIndex.paths must be provided");
                } else {
                    if (paths.size() == 0) {
                        throw new RestException("compoundIndex.paths cannot be empty");
                    }
                    String[] pathsArr = new String[paths.size()];
                    for (int i = 0; i < paths.size(); i++) {
                        pathsArr[i] = paths.get(i).asText();
                    }
                    if (unique == null) {
                        spaceTypeDescriptor.addCompoundIndex(pathsArr);
                    } else {
                        spaceTypeDescriptor.addCompoundIndex(pathsArr, unique);
                    }
                }
            }


            String fifoSupport = getString(actualObj.get("fifoSupport"));
            if (fifoSupport != null) {
                if (!actualObj.get("fifoSupport").isTextual()) {
                    throw new RestException("fifoSupport must be textual");
                }
                FifoSupport fifoSupportEnum;
                try {
                    fifoSupportEnum = FifoSupport.valueOf(fifoSupport);
                } catch (IllegalArgumentException e) {
                    throw new RestException("Illegal fifoSupport: " + e.getMessage());
                }
                spaceTypeDescriptor.fifoSupport(fifoSupportEnum);
            }


            Boolean blobStoreEnabled = getBoolean(actualObj.get("blobStoreEnabled"));
            if (blobStoreEnabled != null) {
                if (!actualObj.get("blobStoreEnabled").isBoolean()) {
                    throw new RestException("blobStoreEnabled must be boolean");
                }
                spaceTypeDescriptor.setBlobstoreEnabled(blobStoreEnabled);
            }


            String documentStorageType = getString(actualObj.get("storageType"));
            if (documentStorageType != null) {
                if (!actualObj.get("storageType").isTextual()) {
                    throw new RestException("storageType must be textual");
                }
                StorageType storageType;
                try {
                    storageType = StorageType.valueOf(documentStorageType);
                } catch (IllegalArgumentException e) {
                    throw new RestException("Illegal storageType: " + e.getMessage());
                }
                spaceTypeDescriptor.storageType(storageType);
            }


            Boolean supportsOptimisticLocking = getBoolean(actualObj.get("supportsOptimisticLocking"));
            if (supportsOptimisticLocking != null) {
                if (!actualObj.get("supportsOptimisticLocking").isBoolean()) {
                    throw new RestException("supportsOptimisticLocking must be boolean");
                }
                spaceTypeDescriptor.supportsOptimisticLocking(supportsOptimisticLocking);
            }


            Boolean supportsDynamicProperties = getBoolean(actualObj.get("supportsDynamicProperties"));
            if (supportsDynamicProperties != null) {
                if (!actualObj.get("supportsDynamicProperties").isBoolean()) {
                    throw new RestException("supportsDynamicProperties must be boolean");
                }
                spaceTypeDescriptor.supportsDynamicProperties(supportsDynamicProperties);
            } else {
                spaceTypeDescriptor.supportsDynamicProperties(true);
            }


            HashSet<String> fixedPropertiesNames = new HashSet<String>();
            JsonNode fixedProperties = actualObj.get("fixedProperties");
            if (fixedProperties != null) {
                for (int i = 0; i < fixedProperties.size(); i++) {
                    JsonNode fixedProperty = fixedProperties.get(i);
                    Iterator<String> fixedPropertyIterator = fixedProperty.fieldNames();
                    while (fixedPropertyIterator.hasNext()) {
                        String fieldName = fixedPropertyIterator.next();
                        if (!fieldName.equals("propertyName") && !fieldName.equals("propertyType")
                                && !fieldName.equals("documentSupport") && !fieldName.equals("storageType") && !fieldName.equals("indexType") && !fieldName.equals("uniqueIndex")) {
                            throw new RestException("Unknown field: " + fieldName + " of FixedProperty at index [" + i + "]");
                        }
                    }

                    String propertyName = getString(fixedProperty.get("propertyName"));
                    if (propertyName != null && !fixedProperty.get("propertyName").isTextual()) {
                        throw new RestException("propertyName of FixedProperty at index [" + i + "] must be textual");
                    }
                    String propertyType = getString(fixedProperty.get("propertyType"));
                    if (propertyType != null && !fixedProperty.get("propertyType").isTextual()) {
                        throw new RestException("propertyType of FixedProperty at index [" + i + "] must be textual");
                    }
                    String documentSupport = getString(fixedProperty.get("documentSupport"));
                    if (documentSupport != null && !fixedProperty.get("documentSupport").isTextual()) {
                        throw new RestException("documentSupport of FixedProperty at index [" + i + "] must be textual");
                    }
                    String propertyStorageType = getString(fixedProperty.get("storageType"));
                    if (propertyStorageType != null && !fixedProperty.get("storageType").isTextual()) {
                        throw new RestException("storageType of FixedProperty at index [" + i + "] must be textual");
                    }

                    //addPropertyIndex
                    String indexType = getString(fixedProperty.get("indexType"));
                    if (indexType != null && !(fixedProperty.get("indexType").isTextual())) {
                        throw new RestException("indexType of FixedProperty at index [" + i + "] must be textual");
                    }
                    Boolean uniqueIndex = getBoolean(fixedProperty.get("uniqueIndex"));
                    if (indexType == null && uniqueIndex != null) {
                        throw new RestException("uniqueIndex cannot be used without indexType field in FixedProperty at index [" + i + "]");
                    }
                    if (uniqueIndex != null && !fixedProperty.get("uniqueIndex").isBoolean()) {
                        throw new RestException("uniqueIndex of FixedProperty at index [" + i + "] must be boolean");
                    }
                    if (indexType != null) {
                        SpaceIndexType spaceIndexType;
                        try {
                            spaceIndexType = SpaceIndexType.valueOf(indexType);
                        } catch (IllegalArgumentException e) {
                            throw new RestException("Illegal fixedProperty.indexType: " + e.getMessage());
                        }
                        if (uniqueIndex == null) {
                            spaceTypeDescriptor.addPropertyIndex(propertyName, spaceIndexType);
                        } else {
                            spaceTypeDescriptor.addPropertyIndex(propertyName, spaceIndexType, uniqueIndex);
                        }
                    }


                    if (propertyName == null) {
                        throw new RestException("Missing propertyName in FixedProperty at index [" + i + "]");
                    }
                    if (propertyType == null) {
                        throw new RestException("Missing propertyType in FixedProperty at index [" + i + "]");
                    }
                    if (fixedPropertiesNames.add(propertyName) == false) {
                        throw new KeyAlreadyExistException(propertyName);
                    }
                    Class propertyValueClass = ControllerUtils.getPrimitiveType(propertyType);

                    if (documentSupport == null && propertyStorageType == null) {
                        if (propertyValueClass != null) {
                            spaceTypeDescriptor.addFixedProperty(propertyName, propertyValueClass);
                        } else {
                            spaceTypeDescriptor.addFixedProperty(propertyName, propertyType);
                        }
                    } else if (documentSupport == null && propertyStorageType != null) {
                        throw new RestException("Cannot apply storageType of FixedProperty without specifying documentSupport");
                    } else if (documentSupport != null && propertyStorageType == null) {
                        SpaceDocumentSupport spaceDocumentSupport;
                        try {
                            spaceDocumentSupport = SpaceDocumentSupport.valueOf(documentSupport);
                        } catch (IllegalArgumentException e) {
                            throw new RestException("Illegal fixedProperty.documentSupport: " + e.getMessage());
                        }

                        if (propertyValueClass != null) {
                            spaceTypeDescriptor.addFixedProperty(propertyName, propertyValueClass, spaceDocumentSupport);
                        } else {
                            spaceTypeDescriptor.addFixedProperty(propertyName, propertyType, spaceDocumentSupport);
                        }
                    } else {
                        SpaceDocumentSupport spaceDocumentSupport;
                        try {
                            spaceDocumentSupport = SpaceDocumentSupport.valueOf(documentSupport);
                        } catch (IllegalArgumentException e) {
                            throw new RestException("Illegal fixedProperty.documentSupport: " + e.getMessage());
                        }
                        StorageType storageType;
                        try {
                            storageType = StorageType.valueOf(propertyStorageType);
                        } catch (IllegalArgumentException e) {
                            throw new RestException("Illegal fixedProperty.storageType: " + e.getMessage());
                        }
                        if (propertyValueClass != null) {
                            spaceTypeDescriptor.addFixedProperty(propertyName, propertyValueClass, spaceDocumentSupport, storageType);
                        } else {
                            spaceTypeDescriptor.addFixedProperty(propertyName, propertyType, spaceDocumentSupport, storageType);
                        }
                    }

                }
            }

            GigaSpace gigaSpace = ControllerUtils.xapCache.get();
            SpaceTypeDescriptor typeDescriptor = gigaSpace.getTypeManager().getTypeDescriptor(type);
            if (typeDescriptor != null) {
                throw new TypeAlreadyRegisteredException(type);
            }

            gigaSpace.getTypeManager().registerTypeDescriptor(spaceTypeDescriptor.create());

            HashMap<String, Object> result = new HashMap<String, Object>();
            result.put("status", "success");
            return result;
        } catch (IOException e) {
            throw new RestException(e.getMessage());

        } catch (KeyAlreadyExistException e) {
            throw new RestException(e.getMessage());
        }
    }

    /**
     * REST GET by query request handler
     *
     * @return Map<String, Object>
     */
    @ApiMethod(
            path = "{type}/",
            verb = ApiVerb.GET,
            description = "Read multiple entries from space that matches the query."
            , produces = {MediaType.APPLICATION_JSON_VALUE}
    )
    @RequestMapping(value = "/{type}", method = RequestMethod.GET, produces = {MediaType.APPLICATION_JSON_VALUE})
    public
    @ResponseBody
    Map<String, Object> getByQuery(
            @PathVariable() @ApiPathParam(name = "type", description = TYPE_DESCRIPTION) String type,
            @RequestParam(value = QUERY_PARAM, required = false)
            @ApiQueryParam(name = "query", description = "a SQLQuery that is a SQL-like syntax") String query,
            @RequestParam(value = MAX_PARAM, required = false)
            @ApiQueryParam(name = "size", description = "") Integer size) throws ObjectNotFoundException {
        if (logger.isLoggable(Level.FINE))
            logger.fine("creating read query with type: " + type + " and query: " + query);

        if (query == null) {
            query = ""; //Query all the data
        }

        GigaSpace gigaSpace = ControllerUtils.xapCache.get();
        SQLQuery<Object> sqlQuery = new SQLQuery<Object>(type, query);
        int maxSize = (size == null ? maxReturnValues : size.intValue());
        Object[] docs;
        try {
            docs = gigaSpace.readMultiple(sqlQuery, maxSize);
        } catch (DataAccessException e) {
            throw translateDataAccessException(gigaSpace, e, type);
        }

        try {
            Map<String, Object> result = new HashMap<String, Object>();
            result.put("status", "success");
            result.put("data", ControllerUtils.mapper.readValue(ControllerUtils.mapper.writeValueAsString(docs), ArrayList.class));
            return result;
        } catch (IOException e) {
            throw new RestException(e.getMessage());
        }
    }

    /**
     * REST GET by ID request handler
     *
     * @return Map<String, Object>
     */
    @ApiMethod(
            path = "{type}/{id}",
            verb = ApiVerb.GET,
            description = "Read entry from space with the provided id"
            , produces = {MediaType.APPLICATION_JSON_VALUE}
    )
    @RequestMapping(value = "/{type}/{id}", method = RequestMethod.GET, produces = {MediaType.APPLICATION_JSON_VALUE})
    public
    @ResponseBody
    Map<String, Object> getById(
            @PathVariable @ApiPathParam(name = "type", description = TYPE_DESCRIPTION) String type,
            @PathVariable @ApiPathParam(name = "id") String id) throws ObjectNotFoundException {
        GigaSpace gigaSpace = ControllerUtils.xapCache.get();
        //read by id request
        Object typedBasedId = getTypeBasedIdObject(gigaSpace, type, id);
        if (logger.isLoggable(Level.FINE))
            logger.fine("creating readbyid query with type: " + type + " and id: " + id);
        IdQuery<Object> idQuery = new IdQuery<Object>(type, typedBasedId);
        Object doc;
        try {
            doc = gigaSpace.readById(idQuery);
        } catch (DataAccessException e) {
            throw translateDataAccessException(gigaSpace, e, type);
        }

        if (doc == null) {
            doc = emptyObject;
        }

        try {
            Map<String, Object> result = new LinkedHashMap<String, Object>();
            result.put("status", "success");
            result.put("data", ControllerUtils.mapper.readValue(ControllerUtils.mapper.writeValueAsString(doc), LinkedHashMap.class));
            return result;
        } catch (IOException e) {
            throw new RestException(e.getMessage());
        }
    }

    /**
     * REST COUNT request handler
     */
    @ApiMethod(
            path = "{type}/count",
            verb = ApiVerb.GET,
            description = "Returns the number of entries in space of the specified type\n", produces = {MediaType.APPLICATION_JSON_VALUE}
    )
    @RequestMapping(value = "/{type}/count", method = RequestMethod.GET
            , produces = {MediaType.APPLICATION_JSON_VALUE})
    public
    @ResponseBody
    Map<String, Object> count(
            @ApiPathParam(name = "type", description = TYPE_DESCRIPTION)
            @PathVariable String type) throws ObjectNotFoundException {

        GigaSpace gigaSpace = ControllerUtils.xapCache.get();
        //read by id request
        Integer cnt;
        try {
            cnt = gigaSpace.count(new SpaceDocument(type));
        } catch (DataAccessException e) {
            throw translateDataAccessException(gigaSpace, e, type);
        }

        if (cnt == null) {
            cnt = 0;
        }
        Map<String, Object> result = new Hashtable<String, Object>();
        result.put("status", "success");
        result.put("data", cnt);
        return result;
    }

    private Object getTypeBasedIdObject(GigaSpace gigaSpace, String type, String id) {
        SpaceTypeDescriptor typeDescriptor = gigaSpace.getTypeManager().getTypeDescriptor(type);
        if (typeDescriptor == null) {
            throw new TypeNotFoundException(type);
        }

        //Investigate id type
        String idPropertyName = typeDescriptor.getIdPropertyName();
        SpacePropertyDescriptor idProperty = typeDescriptor.getFixedProperty(idPropertyName);
        try {
            return ControllerUtils.convertPropertyToPrimitiveType(id, idProperty.getType(), idPropertyName);
        } catch (UnsupportedTypeException e) {
            throw new UnsupportedTypeException("Only primitive SpaceId is currently supported by xap-rest") {
            };
        }
    }


    /**
     * REST DELETE by id request handler
     *
     * @return Map<String, Object>
     */
    @ApiMethod(
            path = "{type}/{id}",
            verb = ApiVerb.DELETE,
            description = "Gets and deletes the entry from space with the provided id."
            , produces = {MediaType.APPLICATION_JSON_VALUE}
    )
    @RequestMapping(value = "/{type}/{id}", method = RequestMethod.DELETE
            , produces = {MediaType.APPLICATION_JSON_VALUE})
    public
    @ResponseBody
    Map<String, Object> deleteById(
            @ApiPathParam(name = "type", description = TYPE_DESCRIPTION)
            @PathVariable String type,
            @ApiPathParam(name = "id")
            @PathVariable String id) throws ObjectNotFoundException {

        GigaSpace gigaSpace = ControllerUtils.xapCache.get();
        //take by id
        Object typedBasedId = getTypeBasedIdObject(gigaSpace, type, id);
        if (logger.isLoggable(Level.FINE))
            logger.fine("creating takebyid query with type: " + type + " and id: " + id);
        Object doc;
        try {
            doc = gigaSpace.takeById(new IdQuery<Object>(type, typedBasedId));
        } catch (DataAccessException e) {
            throw translateDataAccessException(gigaSpace, e, type);
        }

        if (doc == null) {
            doc = emptyObject;
        }

        try {
            Map<String, Object> result = new HashMap<String, Object>();
            result.put("status", "success");
            result.put("data", ControllerUtils.mapper.readValue(ControllerUtils.mapper.writeValueAsString(doc), Map.class));
            return result;
        } catch (IOException e) {
            throw new RestException(e.getMessage());
        }
    }

    /**
     * REST DELETE by query request handler
     *
     * @return Map<String, Object>
     */
    @ApiMethod(
            path = "{type}/",
            verb = ApiVerb.DELETE,
            description = "Gets and deletes entries from space that matches the query."
            , produces = {MediaType.APPLICATION_JSON_VALUE}
    )
    @RequestMapping(value = "/{type}", method = RequestMethod.DELETE
            , produces = {MediaType.APPLICATION_JSON_VALUE})
    public
    @ResponseBody
    Map<String, Object> deleteByQuery(
            @ApiPathParam(name = "type", description = TYPE_DESCRIPTION)
            @PathVariable String type,
            @ApiQueryParam(name = "query")
            @RequestParam(value = QUERY_PARAM) String query,
            @ApiQueryParam(name = "max", description = "The maximum number of entries to return. Default is Integer.MAX_VALUE")
            @RequestParam(value = MAX_PARAM, required = false) Integer max) {
        if (logger.isLoggable(Level.FINE))
            logger.fine("creating take query with type: " + type + " and query: " + query);

        GigaSpace gigaSpace = ControllerUtils.xapCache.get();
        SQLQuery<Object> sqlQuery = new SQLQuery<Object>(type, query);
        int maxSize = (max == null ? maxReturnValues : max.intValue());
        Object[] docs;
        try {
            docs = gigaSpace.takeMultiple(sqlQuery, maxSize);
        } catch (DataAccessException e) {
            throw translateDataAccessException(gigaSpace, e, type);
        }

        if (docs == null) {
            docs = new Object[]{};
        }

        try {
            Map<String, Object> result = new HashMap<String, Object>();
            result.put("status", "success");
            result.put("data", ControllerUtils.mapper.readValue(ControllerUtils.mapper.writeValueAsString(docs), ArrayList.class));
            return result;
        } catch (IOException e) {
            throw new RestException(e.getMessage());
        }
    }

    /**
     * REST POST request handler
     *
     * @return Map<String, Object>
     */
    @ApiMethod(
            path = "{type}/",
            verb = ApiVerb.POST,
            description = "Write one or more entries to the space."
            , consumes = {MediaType.APPLICATION_JSON_VALUE}, produces = {MediaType.APPLICATION_JSON_VALUE}
    )
    @RequestMapping(value = "/{type}", method = RequestMethod.POST
            , consumes = {MediaType.APPLICATION_JSON_VALUE}, produces = {MediaType.APPLICATION_JSON_VALUE})
    public
    @ResponseBody
    Map<String, Object> post(
            @ApiPathParam(name = "type", description = TYPE_DESCRIPTION)
            @PathVariable String type,
            @RequestBody(required = false)
            @ApiBodyObject(clazz = ErrorMessage.class)
            String requestBody)
            throws TypeNotFoundException {
        if (logger.isLoggable(Level.FINE))
            logger.fine("performing post, type: " + type);
        if (requestBody == null) {
            throw new RestException("Request body cannot be empty");
        }
        GigaSpace gigaSpace = ControllerUtils.xapCache.get();
        createAndWriteDocuments(gigaSpace, type, requestBody, WriteModifiers.UPDATE_OR_WRITE);
        Map<String, Object> result = new HashMap<String, Object>();
        result.put("status", "success");
        return result;
    }

    private RuntimeException translateDataAccessException(GigaSpace gigaSpace, DataAccessException e, String type) {
        if (gigaSpace.getTypeManager().getTypeDescriptor(type) == null) {
            return new TypeNotFoundException(type);
        } else {
            return e;
        }
    }

    /**
     * TypeNotFoundException Handler, returns an error response to the client
     */
    @ExceptionHandler(TypeNotFoundException.class)
    @ResponseStatus(value = HttpStatus.NOT_FOUND)
    public
    @ResponseBody
    ErrorResponse resolveTypeDescriptorNotFoundException(TypeNotFoundException e) throws IOException {
        if (logger.isLoggable(Level.FINE))
            logger.fine("type descriptor for typeName: " + e.getTypeName() + " not found, returning error response");

        return new ErrorResponse(new ErrorMessage("Type: " + e.getTypeName() + " is not registered in space"));
    }


    /**
     * ObjectNotFoundException Handler, returns an error response to the client
     */
    @ExceptionHandler(ObjectNotFoundException.class)
    @ResponseStatus(value = HttpStatus.NOT_FOUND)
    public
    @ResponseBody
    ErrorResponse resolveDocumentNotFoundException(ObjectNotFoundException e) throws IOException {
        if (logger.isLoggable(Level.FINE))
            logger.fine("space id query has no results, returning error response: " + e.getMessage());

        return new ErrorResponse(new ErrorMessage(e.getMessage()));
    }

    /**
     * DataAcessException Handler, returns an error response to the client
     */
    @ExceptionHandler(DataAccessException.class)
    @ResponseStatus(value = HttpStatus.INTERNAL_SERVER_ERROR)
    public
    @ResponseBody
    ErrorResponse resolveDataAccessException(DataAccessException e) throws IOException {
        if (logger.isLoggable(Level.WARNING))
            logger.log(Level.WARNING, "received DataAccessException exception", e);

        return new ErrorResponse(new ExceptionMessage(e));

    }

    @ExceptionHandler(CannotFindSpaceException.class)
    @ResponseStatus(value = HttpStatus.INTERNAL_SERVER_ERROR)
    public
    @ResponseBody
    ErrorResponse resolveCannotFindSpaceException(CannotFindSpaceException e) throws IOException {
        if (logger.isLoggable(Level.WARNING))
            logger.log(Level.WARNING, "received CannotFindSpaceException exception", e);

        return new ErrorResponse(new ExceptionMessage(e));
    }

    @ExceptionHandler(TypeAlreadyRegisteredException.class)
    @ResponseStatus(value = HttpStatus.INTERNAL_SERVER_ERROR)
    public
    @ResponseBody
    ErrorResponse
    resoleTypeAlreadyRegisteredException(TypeAlreadyRegisteredException e) throws IOException {
        if (logger.isLoggable(Level.WARNING))
            logger.log(Level.WARNING, "received TypeAlreadyRegisteredException exception", e);

        return new ErrorResponse(new ErrorMessage("Type: " + e.getTypeName() + " is already introduced to space"));
    }

    @ExceptionHandler(RestException.class)
    @ResponseStatus(value = HttpStatus.INTERNAL_SERVER_ERROR)
    public
    @ResponseBody
    ErrorResponse resolveRestIntroduceTypeException(RestException e) throws IOException {
        if (logger.isLoggable(Level.WARNING))
            logger.log(Level.WARNING, "received RestException exception", e.getMessage());

        return new ErrorResponse(new ErrorMessage(e.getMessage()));
    }

    @ExceptionHandler(RuntimeException.class)
    @ResponseStatus(value = HttpStatus.INTERNAL_SERVER_ERROR)
    public
    @ResponseBody
    ErrorResponse resolveRuntimeException(RuntimeException e) throws IOException {
        if (logger.isLoggable(Level.SEVERE))
            logger.log(Level.SEVERE, "received RuntimeException (unhandled) exception", e.getMessage());

        return new ErrorResponse(new ErrorMessage("Unhandled exception [" + e.getClass() + "]: " + e.toString()));
    }

    @ExceptionHandler(UnsupportedTypeException.class)
    @ResponseStatus(value = HttpStatus.INTERNAL_SERVER_ERROR)
    public
    @ResponseBody
    ErrorResponse resolveUnsupportedTypeException(UnsupportedTypeException e) throws IOException {
        if (logger.isLoggable(Level.WARNING))
            logger.log(Level.WARNING, "received UnsupportedTypeException exception", e.getMessage());

        return new ErrorResponse(new ErrorMessage(e.getMessage()));
    }

    /**
     * helper method that creates space documents from the httpRequest payload and writes them to
     * space.
     */
    private void createAndWriteDocuments(GigaSpace gigaSpace, String type, String body, WriteModifiers updateModifiers)
            throws TypeNotFoundException {
        logger.info("creating space Documents from payload");
        SpaceDocument[] spaceDocuments = ControllerUtils.createSpaceDocuments(type, body, gigaSpace);
        if (spaceDocuments != null && spaceDocuments.length > 0) {
            try {
                gigaSpace.writeMultiple(spaceDocuments, Lease.FOREVER, updateModifiers);
            } catch (DataAccessException e) {
                throw translateDataAccessException(gigaSpace, e, type);
            }
            if (logger.isLoggable(Level.FINE))
                logger.fine("wrote space documents to space");
        } else {
            if (logger.isLoggable(Level.FINE))
                logger.fine("did not write anything to space");
        }
    }


}
