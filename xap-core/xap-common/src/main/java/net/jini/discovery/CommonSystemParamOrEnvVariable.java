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
package net.jini.discovery;

import com.gigaspaces.internal.utils.GsEnv;

public class CommonSystemParamOrEnvVariable {

    public static int getIntFromEnv(String envKey,String properyKey, int intDefault){
        String valFromEnv = getFromEnv(envKey);

        return valFromEnv != null ?
                Integer.valueOf(valFromEnv) :
                Integer.getInteger(properyKey, intDefault);
    }

    public static String getStringFromEnv(String envKey,String properyKey, String strDefault){
        String valFromEnv = getFromEnv(envKey);

        return valFromEnv != null ?
                valFromEnv :
                System.getProperty(properyKey, strDefault);
    }

    private static String getFromEnv(String key){
         return GsEnv.get(key);
    }
}
