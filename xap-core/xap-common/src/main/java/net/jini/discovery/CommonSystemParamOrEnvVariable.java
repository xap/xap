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
