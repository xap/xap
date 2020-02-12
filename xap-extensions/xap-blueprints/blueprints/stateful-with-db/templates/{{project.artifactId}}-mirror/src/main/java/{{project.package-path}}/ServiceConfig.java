package {{project.groupId}};

import org.openspaces.config.DefaultServiceConfig;
import org.springframework.context.annotation.*;

@Configuration
@Import({DefaultServiceConfig.class, CustomMirrorConfig.class})
public class ServiceConfig {
}
