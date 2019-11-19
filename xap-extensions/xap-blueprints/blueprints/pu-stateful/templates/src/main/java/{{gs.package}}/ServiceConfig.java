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
