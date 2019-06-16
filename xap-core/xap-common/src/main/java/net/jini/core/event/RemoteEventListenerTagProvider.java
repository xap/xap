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
package net.jini.core.event;

/**
 * Tagging interface on top of {@link RemoteEventListener} that provides additional information
 * (e.g. tag) to be extracted for debugging/logging purposes for a reliable replication target.
 *
 * @since 14.5
 */
public interface RemoteEventListenerTagProvider {

    /**
     * @return A name/id/tag identifying this remote event listener endpoint
     * @since 14.5
     */
    String getTag();
}
