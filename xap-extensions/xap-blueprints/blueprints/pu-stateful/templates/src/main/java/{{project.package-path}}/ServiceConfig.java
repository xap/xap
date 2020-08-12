package {{project.groupId}};

import org.openspaces.config.DefaultServiceConfig;
import org.openspaces.core.config.annotation.EmbeddedSpaceBeansConfig;
import org.springframework.context.annotation.*;

@Configuration
@Import({DefaultServiceConfig.class, EmbeddedSpaceBeansConfig.class})
public class ServiceConfig {
    @Bean
    MyBean myBean() {
        return new MyBean();
    }
}
