package {{maven.groupId}};

import org.openspaces.core.GigaSpace;
import org.openspaces.core.GigaSpaceConfigurer;
import org.openspaces.core.space.EmbeddedSpaceConfigurer;

public class Program {
    public static void main(String[] args) {
        System.out.println("Creating embedded space {{spaceName}}");

        GigaSpace gigaSpace = new GigaSpaceConfigurer(
                new EmbeddedSpaceConfigurer("{{spaceName}}"))
                .create();

        System.out.println("Created embedded space {{spaceName}}");
    }
}
