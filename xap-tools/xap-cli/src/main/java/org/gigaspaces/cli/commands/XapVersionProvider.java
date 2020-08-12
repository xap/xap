package org.gigaspaces.cli.commands;

import com.gigaspaces.internal.version.PlatformVersion;
import picocli.CommandLine;

public class XapVersionProvider implements CommandLine.IVersionProvider {
    @Override
    public String[] getVersion() throws Exception {
        return new String[] {
            PlatformVersion.getOfficialVersion()
        };
    }
}
