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
