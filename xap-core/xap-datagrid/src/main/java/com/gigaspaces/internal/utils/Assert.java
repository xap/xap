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

package com.gigaspaces.internal.utils;

/**
 * Assert utility class that assists in validating arguments. Useful for identifying programmer
 * errors early and obviously at runtime.
 *
 * <p>For example, if the contract of a public method states it does not allow <code>null</code>
 * arguments, Assert can be used to validate that contract. Doing this clearly indicates a contract
 * violation when it occurs and protects the class's invariants.
 *
 * <p>Typically used to validate method arguments rather than configuration properties, to check for
 * cases that are usually programmer errors rather than configuration errors. In contrast to config
 * initialization code, there is usally no point in falling back to defaults in such methods.
 *
 * <p>This class is similar to JUnit's assertion library. If an argument value is deemed invalid, an
 * {@link IllegalArgumentException} is thrown (typically). For example:
 *
 * <pre class="code"> Assert.notNull(clazz, "The class must not be null"); Assert.isTrue(i > 0, "The
 * value must be greater than zero");</pre>
 */
public abstract class Assert {
}
