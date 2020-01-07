package {{project.groupId}};

import org.openspaces.core.*;
import org.openspaces.core.space.*;

public class Program {
    public static void main(String[] args) {
        GigaSpace gigaSpace = getOrCreateSpace(args.length == 0 ? null : args[0]);
        System.out.println("Connected to space " + gigaSpace.getName());

        // Your code goes here, for example:
        System.out.println("Entries in space: " + gigaSpace.count(null));

        System.out.println("Program completed successfully");
        System.exit(0);
    }

    public static GigaSpace getOrCreateSpace(String spaceName) {
        if (spaceName == null) {
            System.out.println("Space name not provided - creating an embedded space...");
            return new GigaSpaceConfigurer(new EmbeddedSpaceConfigurer("mySpace")).create();
        } else {
            System.out.printf("Connecting to space %s...%n", spaceName);
            try {
                return new GigaSpaceConfigurer(new SpaceProxyConfigurer(spaceName)).create();
            } catch (CannotFindSpaceException e) {
                System.err.println("Failed to find space: " + e.getMessage());
                throw e;
            }
        }
    }
}
