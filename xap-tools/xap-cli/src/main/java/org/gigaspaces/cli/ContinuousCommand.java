package org.gigaspaces.cli;

/**
 * Indicates that a command is a continous command (i.e. does not terminate on its own), and should be run on a separate
 * window when launched from an interactive shell.
 *
 * @author Niv Ingberg
 * @since 15.2
 */
public interface ContinuousCommand {
    default void validate() throws CliCommandException {
    }
}
