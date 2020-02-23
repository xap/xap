/*
 * 
 * Copyright 2005 Sun Microsystems, Inc.
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 * 	http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * 
 */

package com.sun.jini.logging;

import java.util.logging.Level;

/**
 * Defines additional {@link Level} values. <p>
 *
 * See the {@link LogManager} class for one way to use the <code>FAILED</code> and
 * <code>HANDLED</code> logging levels in standard logging configuration files.
 *
 * @author Sun Microsystems, Inc.
 * @since 2.0
 */
@com.gigaspaces.api.InternalApi
public class Levels {

    /**
     * <code>HANDLED</code> is a message level indicating that a facility has detected a failure
     * that it will take steps to handle without reflecting the failure to its caller. <p>
     *
     * <code>HANDLED</code> messages are intended to provide users with information about failures
     * detected by internal components in order to assist with debugging problems in systems with
     * multiple components. This level is initialized to <code>550</code>.
     */
    public static final Level HANDLED = new LevelData("HANDLED", 550, null);

    /**
     * This class cannot be instantiated.
     */
    private Levels() {
        throw new AssertionError("This class cannot be instantiated");
    }

    /**
     * Defines a class that has the same data format as the Level class, to permit creating the
     * serialized form of a Level instance.
     */
    private static final class LevelData extends Level {
        private static final long serialVersionUID = -5191742979032167352L;

        public LevelData(String name, int value, String resourceBundleName) {
            super(name, value, resourceBundleName);
        }
    }
}
