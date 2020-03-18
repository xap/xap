/*
 * Copyright (c) 2008-2016, GigaSpaces Technologies, Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package {{project.groupId}};

import org.slf4j.*;
import javax.annotation.*;
import org.springframework.stereotype.Component;

import org.openspaces.core.*;
import org.openspaces.core.space.*;
import org.openspaces.core.space.status.*;
import org.openspaces.core.cluster.*;

@Component
public class MyBean {
    private static final Logger logger = LoggerFactory.getLogger(MyBean.class);

    @Resource
    private GigaSpace gigaSpace;

    @ClusterInfoContext
    private ClusterInfo clusterInfo;

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
