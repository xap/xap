package {{project.groupId}};

import org.openspaces.config.DefaultServiceConfig;
import org.openspaces.core.config.annotation.SpaceProxyBeansConfig;
import org.springframework.context.annotation.*;

@Configuration
@Import({DefaultServiceConfig.class, SpaceProxyBeansConfig.class})
public class ServiceConfig {
    @Bean
    MyBean myBean() {
        return new MyBean();
    }
}
