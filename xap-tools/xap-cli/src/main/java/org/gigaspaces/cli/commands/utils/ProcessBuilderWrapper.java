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
package org.gigaspaces.cli.commands.utils;

import java.io.IOException;

/**
 * @since 12.3
 * @author evgeny
 */
public class ProcessBuilderWrapper {

    private final ProcessBuilder processBuilder;
    private final Object lock = new Object();
    private Process process;
    private boolean destroyed;

    public ProcessBuilderWrapper(ProcessBuilder processBuilder ){
        this.processBuilder = processBuilder;
    }

    public Process start() throws IOException {
        synchronized (lock) {
            if (destroyed)
                throw new IOException("Cannot create process - already destroyed");
            this.process = processBuilder.start();
            return process;
        }
    }

    public Process destroy() {
        synchronized (lock) {
            destroyed = true;
            if (process != null)
                process.destroy();
            return process;
        }
    }

    public boolean allowToStart(){
    return true;
  }

  public boolean isSyncCommand(){
    return true;
  }
}