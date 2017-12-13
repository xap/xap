package org.gigaspaces.cli;

import java.util.Collection;

public interface SubCommandContainer {
    Collection<Object> getSubCommands();
}
