/*
 * Copyright (c) 2008-2019, GigaSpaces Technologies, Inc. All Rights Reserved.
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
package org.openspaces.core.config.annotation;

import org.openspaces.core.space.mode.registry.ModeAnnotationRegistry;
import org.openspaces.core.space.mode.registry.ModeAnnotationRegistryPostProcessor;
import org.openspaces.core.space.status.registery.SpaceStatusChangedAnnotationRegistry;
import org.openspaces.core.space.status.registery.SpaceStatusChangedAnnotationRegistryPostProcessor;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * @author Niv Ingberg
 * @since 15.0
 */
@Configuration
public class CoreAnnotationsBeansConfig {
    @Bean("internal-modeAnnotationRegistry")
    ModeAnnotationRegistry modeAnnotationRegistry() {
        return new ModeAnnotationRegistry();
    }

    @Bean("internal-modeAnnotationRegistryPostProcessor")
    ModeAnnotationRegistryPostProcessor modeAnnotationRegistryPostProcessor() {
        return new ModeAnnotationRegistryPostProcessor();
    }

    @Bean("internal-spaceStatusAnnotationRegistry")
    SpaceStatusChangedAnnotationRegistry spaceStatusChangedAnnotationRegistry() {
        return new SpaceStatusChangedAnnotationRegistry();
    }

    @Bean("internal-spaceStatusAnnotationRegistryPostProcessor")
    SpaceStatusChangedAnnotationRegistryPostProcessor spaceStatusChangedAnnotationRegistryPostProcessor() {
        return new SpaceStatusChangedAnnotationRegistryPostProcessor();
    }
}
