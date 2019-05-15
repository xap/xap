package {{maven.groupId}};

import org.openspaces.core.GigaSpace;
import org.openspaces.core.GigaSpaceConfigurer;
import org.openspaces.core.space.CannotFindSpaceException;
import org.openspaces.core.space.EmbeddedSpaceConfigurer;
import org.openspaces.core.space.SpaceConfigurer;
import org.openspaces.core.space.SpaceProxyConfigurer;

import {{maven.groupId}}.demo.Demo;

public class Program {
    public static void main(String[] args) {
        SpaceConfigurer spaceConfigurer;
        if (args.length == 0) {
            System.out.println("Space name not provided - creating an embedded space...");
            spaceConfigurer = new EmbeddedSpaceConfigurer("mySpace");
        } else {
            String spaceName = args[0];
            System.out.printf("Connecting to space %s...%n", spaceName);
            spaceConfigurer = new SpaceProxyConfigurer(spaceName);
        }

        try {
            GigaSpace gigaSpace = new GigaSpaceConfigurer(spaceConfigurer).create();
            System.out.println("Connected to space, running demo...");
            Demo.run(gigaSpace);
        } catch (CannotFindSpaceException e) {
            System.err.println("Failed to find space: " + e.getMessage());
        }

        spaceConfigurer.close();
        System.out.println("Program completed successfully");
        System.exit(0);
    }
}
