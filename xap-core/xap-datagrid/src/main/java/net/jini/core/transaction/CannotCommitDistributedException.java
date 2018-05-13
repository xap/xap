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
package net.jini.core.transaction;


import com.j_spaces.kernel.JSpaceUtilities;

import java.util.Map;

/**
 * Exception thrown when a transaction cannot commit on multiple partitions because it has already
 * aborted or must now be aborted.
 *
 * @author Yael Nahon
 *
 * @since 12.1
 */
public class CannotCommitDistributedException extends CannotCommitException {
    static final long serialVersionUID = 1L;
    private Map<Integer, Exception> exceptions;
    /**
     * Constructs an instance with a detail message.
     *
     * @param desc the detail message
     */
    public CannotCommitDistributedException(String desc, Map<Integer, Exception> participantsExceptions) {
        super(desc);
        exceptions = participantsExceptions;
    }

    /** Constructs an instance with no detail message. */
    public CannotCommitDistributedException() {
        super();
    }

    public Map<Integer, Exception> getParticipantsExceptions(){
        return exceptions;
    }

    public static CannotCommitDistributedException getCannotCommitDistributedException(Exception e) {
        return (CannotCommitDistributedException) JSpaceUtilities.getAssignableCauseExceptionFromHierarchy(e, CannotCommitDistributedException.class);
    }
}
