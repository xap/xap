package {{project.groupId}};

import org.slf4j.*;
import javax.annotation.*;

import org.openspaces.core.*;
import org.openspaces.core.space.status.*;
import org.openspaces.core.cluster.*;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.ApplicationContext;

import java.util.Arrays;

public class MyBean {
    private final Logger logger = LoggerFactory.getLogger(this.getClass().getName());

    @Resource // Injected by Spring
    private GigaSpace gigaSpace;

    @ClusterInfoContext //Injected by GigaSpaces
    private ClusterInfo clusterInfo;

    @Value("${space.name}") // Injected by Spring
    private String spaceName;

    private String id;

    @PostConstruct
    public void initialize() {
        id = gigaSpace.getSpaceName() + "[" + (clusterInfo != null ? clusterInfo.getSuffix() : "non-clustered") + "]";
        logger.info("Initialized {}", id);
        // NOTE: This method is called for both primary and backup instances.
        // If you wish to do something for primaries only, see @SpaceStatusChanged
    }

    @SpaceStatusChanged
    public void onSpaceStatusChange(SpaceStatusChangedEvent event) {
        logger.info("Space {} is {}", id, event.getSpaceMode());
        if (event.isActive()) {
            // If you have initialization code for active instances only, put it here.
        } else {
            // Space is backup, or space is primary but suspended.
            // If your code should only run when the space is active, you should deactivate it here.
        }
    }

    @PreDestroy
    public void close() {
        logger.info("Closing {}", id);
    }
}
