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

package com.gigaspaces.internal.utils;

import com.gigaspaces.security.authorities.GridAuthority.GridPrivilege;
import com.j_spaces.core.exception.InvalidServiceNameException;

@com.gigaspaces.api.InternalApi
public class ValidationUtils {
    // array of characters that cannot be part of space name
    private final static char[] INVALID_CHARACTERS_ARRAY = {'>', '<',
            ',', '"', '\'', '&', '\\', '!', '@', '#', '$', '%', '^', '*', '~',
            '+', ')', '('};

    private final static String INVALID_CHARACTERS_STR;
    // message for invalid characters
    private final static String INVALID_CHARACTERS_MESSAGE_SUFFIX = " cannot contain a space or any of the following characters:";
    // message for names that start with digit
    private final static String INVALID_DIGITS_MESSAGE_SUFFIX = " cannot start with digits.";

    public final static String MSG_UNABLE_TO_DEPLOY_THEN_LOGIN_TO_GSM =
            "You are unauthorized to deploy onto a secured GSM without the <" +
                    GridPrivilege.PROVISION_PU +
                    "> permission. Do you wish to login?";

    public final static String MSG_UNABLE_TO_DEPLOY_THEN_LOGIN_TO_GSM_ALREADY_LOOGED_IN =
            "You are unauthorized to deploy onto a secured GSM without the <" +
                    GridPrivilege.PROVISION_PU +
                    "> permission.\nPlease login as a different user.";


    private final static String MSG_UNABLE_TO_DEPLOY_READ_ONLY_GSM =
            "Authentication failure. Unable to deploy processing unit without the <" +
                    GridPrivilege.PROVISION_PU + "> permission.";

    private final static String MSG_UNABLE_TO_DEPLOY_USER_DOES_NOT_EXIST =
            "Unable to perform deployment. Authentication failed.";


    public final static String MSG_UNABLE_TO_RELOCATE_PREFIX =
            "You do not have sufficient permissions to relocate the processing unit instance ";

    public final static String MSG_UNABLE_TO_RELOCATE_SUFFIX =
            "This operation requires the &lt;" + GridPrivilege.MANAGE_PU + "&gt; permission.";

    public final static String MSG_UNABLE_TO_RELOCATE_LOGIN_SUFFIX =
            "Do you wish to login?";

    public final static String MSG_UNABLE_TO_RELOCATE_LOGIN_AS_DIFFERENT_USER_SUFFIX =
            "Do you wish to login as a different user?";


    public final static String MSG_UNABLE_TO_DEPLOY_SINGLE_SPACE =
            "Unable to deploy space; User lacks [" +
                    GridPrivilege.PROVISION_PU + "] privileges for available GSMs.";


    static {
        StringBuilder strBuffer = new StringBuilder();
        // go through characters array and add them to string separated by space
        for (int i = 0; i < INVALID_CHARACTERS_ARRAY.length; i++) {
            strBuffer.append(INVALID_CHARACTERS_ARRAY[i]);
            strBuffer.append(' ');
        }
        INVALID_CHARACTERS_STR = strBuffer.toString();
    }


    public static void checkServiceNameForValidation(String serviceNameVal, String name)
            throws InvalidServiceNameException {

        if (serviceNameVal.indexOf(' ') >= 0
                || isStringHasInvalidCharacters(serviceNameVal)) {
            String exceptionMessage =
                    name + INVALID_CHARACTERS_MESSAGE_SUFFIX +
                            "\n" + INVALID_CHARACTERS_STR + ".";
            if (isSringStartsWithNumber(serviceNameVal)) {
                exceptionMessage = createInvalidDigitsAndCharactersMessage(name);
            }

            throw new InvalidServiceNameException(exceptionMessage);
        }

        if (isSringStartsWithNumber(serviceNameVal)) {
            String exceptionMessage = name + INVALID_DIGITS_MESSAGE_SUFFIX;
            throw new InvalidServiceNameException(exceptionMessage);
        }
    }

    public static boolean isStringHasInvalidCharacters(String str) {
        for (int i = 0; i < INVALID_CHARACTERS_ARRAY.length; i++) {
            char invalidChar = INVALID_CHARACTERS_ARRAY[i];
            if (str.indexOf(invalidChar) >= 0)
                return true;
        }

        return false;
    }

    public static boolean isSringStartsWithNumber(String str) {
        try {
            String firstChar = str.substring(0, 1);
            Integer.parseInt(firstChar);
        } catch (NumberFormatException e) {
            return false;
        }

        return true;
    }

    private static String createInvalidDigitsAndCharactersMessage(String name) {
        StringBuilder strBuffer =
                new StringBuilder(name);
        strBuffer.append(INVALID_CHARACTERS_MESSAGE_SUFFIX);
        strBuffer.append("\n");
        strBuffer.append(INVALID_CHARACTERS_STR);
        strBuffer.append(".\n");
        strBuffer.append(name);
        strBuffer.append(INVALID_DIGITS_MESSAGE_SUFFIX);


        return strBuffer.toString();
    }

}