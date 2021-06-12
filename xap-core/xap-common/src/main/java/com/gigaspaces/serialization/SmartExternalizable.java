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
package com.gigaspaces.serialization;

import java.io.Externalizable;

/**
 * Externalizable markup extension for GigaSpaces enhanced serialization.
 *
 * When an instance of this interface is serialized using GigaSpaces, the following optimizations take place:
 * 1. The first time it is serialized on a channel, it's assigned an id (int), which is cached on the receiving side of
 * the channel. Subsequent serializations write the id instead of the class descriptor, which reduce cpu and network usage.
 * 2. The first time it is deserialized on a channel, GigaSpaces generates a factory class (using byte-code generation)
 * which creates instances of the class using the default constructor. This factory is used to instantiate deserialized
 * instances instead of reflection, thus reducing cpu usage significantly.
 *
 * This interface is in use by relevant GigaSpaces serializable classes.
 * Users may use this interface on their classes as well. However, keep in mind this enhancement intentionally avoids the
 * standard writeObject/readObject mechanism for performance, including its reference identity mechanism:
 * a) If an instance implementing SmartExternalizable is serialized multiple times in the same operation, it will be
 * serialized separately each time, leading to extra resource usage and possibly unexpected behaviour if the receiving
 * end expects references identity to be preserved.
 * b) If an instance implementing SmartExternalizable is part of a references cycle, the serializing thread will run
 * into an infinite loop and hang.
 * For performance reasons, the SmartExternalizable does not handle those cases - it is the user's responsibility to
 * check if they are relevant for their application when deciding whether or not to use SmartExternalizable
 *
 * @author Niv Ingberg
 * @since 16.0
 */
public interface SmartExternalizable extends Externalizable {
}
