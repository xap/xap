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

package com.j_spaces.core;

import com.gigaspaces.internal.utils.ReplaceInFileUtils;
import com.gigaspaces.start.SystemInfo;
import com.gigaspaces.start.SystemLocations;
import com.j_spaces.core.admin.ContainerConfig;
import com.j_spaces.kernel.ResourceLoader;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.rmi.RemoteException;

import static com.j_spaces.core.Constants.Container.CONTAINER_NAME_PROP;
import static com.j_spaces.core.Constants.LookupManager.LOOKUP_ENABLED_PROP;
import static com.j_spaces.core.Constants.LookupManager.LOOKUP_GROUP_PROP;
import static com.j_spaces.core.Constants.LookupManager.LOOKUP_JMS_ENABLED_PROP;
import static com.j_spaces.core.Constants.LookupManager.LOOKUP_JMS_EXT_ENABLED_PROP;
import static com.j_spaces.core.Constants.LookupManager.LOOKUP_JMS_INTERNAL_ENABLED_PROP;
import static com.j_spaces.core.Constants.LookupManager.LOOKUP_JNDI_ENABLED_PROP;
import static com.j_spaces.core.Constants.LookupManager.LOOKUP_JNDI_URL_PROP;
import static com.j_spaces.core.Constants.LookupManager.LOOKUP_UNICAST_ENABLED_PROP;
import static com.j_spaces.core.Constants.LookupManager.LOOKUP_UNICAST_URL_PROP;
import static com.j_spaces.core.Constants.Schemas.CONTAINER_SCHEMA_FILE_SUFFIX;
import static com.j_spaces.core.Constants.Schemas.SCHEMAS_FOLDER;

/**
 * ContainerConfigFactory
 *
 * @version 1.0
 * @since 5.0
 */
@com.gigaspaces.api.InternalApi
public class ContainerConfigFactory {

    /**
     * Save content of ContainerConfig instance into xml file( schema or configuration ).
     *
     * @param selFilePath In this file configuration will be saved
     */

    public static void performSaveAs(String schemaPrefixFileName, String selFilePath, ContainerConfig containerConfig) {
        try {

            //Write content of default schema to required file
            File fileWithDefaultConfiguration = createContainerSchemaFile(schemaPrefixFileName, selFilePath);

            //replace content of selected file with passed container content that saved in
            //containerConfig instance
            updateFile(containerConfig, fileWithDefaultConfiguration.getPath(), schemaPrefixFileName);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * This method creates container schema.configuration file with default definitions.
     *
     * @return created file
     */
    public static File createContainerSchemaFile(String schemaPrefixFileName, String containerSchemaFilePath) throws RemoteException {
        File defaultSchemaFile = null;

        try {

            String schemaFilePath = Constants.Container.CONTAINER_CONFIG_DIRECTORY + "/"
                    + Constants.Schemas.SCHEMAS_FOLDER + "/" + Constants.Schemas.DEFAULT_SCHEMA
                    + Constants.Schemas.CONTAINER_SCHEMA_FILE_SUFFIX;

            try (InputStream schemaInputStream = ResourceLoader.getResourceStream(schemaFilePath)) {

                String folderOwnerPath =
                        containerSchemaFilePath.substring(
                                0, containerSchemaFilePath.lastIndexOf(File.separator));

                File folderOwnerInstance = new File(folderOwnerPath);
                if (!folderOwnerInstance.exists()) {
                    //create folder owner
                    folderOwnerInstance.mkdirs();
                }

                //create schema file under "schema" directories
                defaultSchemaFile = new File(containerSchemaFilePath);
                //Creates OutputStream for the just created file , in order to write into it
                try (FileOutputStream fos = new FileOutputStream(defaultSchemaFile)) {
                    int read = 1;
                    while (read > 0) {
                        byte[] readBytesArray = new byte[schemaInputStream.available()];
                        //read form input stream
                        read = schemaInputStream.read(readBytesArray);
                        //write to output stream
                        fos.write(readBytesArray);
                    }
                    fos.flush();
                }
            }


            //replace both tags: <default> and </default> with schema prefix name
            ReplaceInFileUtils file = new ReplaceInFileUtils(containerSchemaFilePath);
            file.replaceInFile("<" + Constants.Schemas.DEFAULT_SCHEMA + ">", "<" + schemaPrefixFileName + ">");
            file.replaceInFile("</" + Constants.Schemas.DEFAULT_SCHEMA + ">", "</" + schemaPrefixFileName + ">");
            file.close();
        } catch (Exception ex) {
            throw new RemoteException("Fail to create schema file", ex);
        }
        //return path of created schema file
        return defaultSchemaFile;
    }

    /**
     * Pass through container configuration/schema XML file and change its values according to
     * properties saved in  ContainerConfig instance.
     */
    public static void updateFile(ContainerConfig config, String containerConfigURL, String schemaPrefixFileName) throws IOException {

        ReplaceInFileUtils containerFile = new ReplaceInFileUtils(containerConfigURL);
        //String containerName = config.containerName;

        containerFile.xmlReplace(CONTAINER_NAME_PROP, schemaPrefixFileName);

        containerFile.xmlReplace(LOOKUP_ENABLED_PROP, String.valueOf(config.isJiniLusEnabled()));
        containerFile.xmlReplace(LOOKUP_JNDI_ENABLED_PROP, String.valueOf(config.isJndiEnabled()));

        // JNDI port
        if (config.jndiUrl != null)
            containerFile.xmlReplace(LOOKUP_JNDI_URL_PROP, config.jndiUrl);

        // groups for lookup service
        if (config.lookupGroups != null)
            containerFile.xmlReplace(LOOKUP_GROUP_PROP, config.lookupGroups);

        // unicast is enabled?
        containerFile.xmlReplace(LOOKUP_UNICAST_ENABLED_PROP, String.valueOf(config.unicastEnabled));

        if (config.unicastURL != null)
            containerFile.xmlReplace(LOOKUP_UNICAST_URL_PROP, config.unicastURL);

        // JMS settings
        containerFile.xmlReplace(LOOKUP_JMS_ENABLED_PROP, String.valueOf(config.jmsEnabled));
        containerFile.xmlReplace(LOOKUP_JMS_INTERNAL_ENABLED_PROP, String.valueOf(config.jmsInternalJndiEnabled));
        containerFile.xmlReplace(LOOKUP_JMS_EXT_ENABLED_PROP, String.valueOf(config.jmsExtJndiEnabled));

        // save and close config xml file
        containerFile.close();
    }

    public static String getContainerSchemaPathName(String containerName) {
        StringBuilder result = new StringBuilder();
        result.append(SystemLocations.singleton().config());
        result.append(File.separator);
        result.append(SCHEMAS_FOLDER);
        result.append(File.separator);
        result.append(containerName);
        result.append(CONTAINER_SCHEMA_FILE_SUFFIX);
        return result.toString();
    }

}
