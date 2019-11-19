package {{maven.groupId}};

import org.openspaces.config.DefaultServiceConfig;
import org.openspaces.core.config.annotation.EmbeddedSpaceBeansConfig;
import org.slf4j.*;
import org.springframework.context.annotation.*;

@Configuration
@Import({DefaultServiceConfig.class, EmbeddedSpaceBeansConfig.class})
public class ServiceConfig {
    private final Logger logger = LoggerFactory.getLogger(this.getClass().getName());

    @Bean
    MyBean myBean() {
        logger.info("*** myBean");
        return new MyBean();
    }
}
