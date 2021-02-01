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
package org.gigaspaces.cli;

public class CliCommandException extends Exception {
    public static final int CODE_GENERAL_ERROR = 1;
    public static final int CODE_INVALID_INPUT = 2;
    public static final int CODE_TIMEOUT = 6;

    private int exitCode = CODE_GENERAL_ERROR;

    public CliCommandException(String msg) {
        super(msg);
    }

    public CliCommandException(String msg, Throwable cause) {
        super(msg, cause);
    }

    public static CliCommandException userError(String message) {
        return new CliCommandException(message).exitCode(CODE_INVALID_INPUT);
    }

    public int getExitCode() {
        return exitCode;
    }

    public CliCommandException exitCode(int exitCode) {
        this.exitCode = exitCode;
        return this;
    }

    public boolean isUserError() {
        return exitCode == CODE_INVALID_INPUT;
    }

    public boolean isTimeoutError() {
        return exitCode == CODE_TIMEOUT;
    }
}
