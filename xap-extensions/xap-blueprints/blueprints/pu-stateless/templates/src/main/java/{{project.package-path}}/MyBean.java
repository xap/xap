package {{project.groupId}};

import org.slf4j.*;
import javax.annotation.*;

import org.openspaces.core.*;
import org.openspaces.core.space.*;

public class MyBean {
    private static final Logger logger = LoggerFactory.getLogger(MyBean.class);

    @Resource
    private GigaSpace gigaSpace;

    @PostConstruct
    public void initialize() {
        logger.info("Initialized: connected to space {}", gigaSpace.getSpaceName());
        // Your code goes here, for example:
        int count = gigaSpace.count(null);
        logger.info("Entries in space: {}", count);
    }

    @PreDestroy
    public void close() {
        logger.info("Closing");
    }
}
