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


package com.gigaspaces.annotation.pojo;

import com.gigaspaces.internal.metadata.PojoDefaults;
import com.gigaspaces.metadata.StorageType;

import java.lang.annotation.Retention;
import java.lang.annotation.Target;

import static java.lang.annotation.ElementType.TYPE;
import static java.lang.annotation.RetentionPolicy.RUNTIME;

/**
 * <pre>
 * Defines a class annotation of Entry/POJO
 * objects. This class is annotations class describe the fields we are using for
 * convert POJO to entry and entry to POJO with out gs xml file The annotation
 * indicate POJO to entry attribute
 * </pre>
 * <p>
 *
 * @author Lior Ben Yizhak
 * @since 5.0
 */
@Target(TYPE)
@Retention(RUNTIME)
public @interface SpaceClass {
    public enum IncludeProperties {EXPLICIT, IMPLICIT}

    /**
     * Determines FIFO operations support for this class.
     *
     * @see FifoSupport
     */
    FifoSupport fifoSupport() default FifoSupport.DEFAULT;

    /**
     * When running in partial replication mode, a true value for this field will replicate the all
     * object of this type to target space(s)
     */
    boolean replicate() default PojoDefaults.REPLICATE;

    /**
     * When space defined as persistent true value for this annotation will persist object of this
     * type. See more at the database cache and persistency section.
     */
    boolean persist() default PojoDefaults.PERSIST;

    /**
     * The index level indicate if to take into account the indexes which declared into superclasses
     * or only in the instance itself
     *
     * @return boolean true for inherit.
     * @deprecated Since 8.0 - To disable an index from the super class, override the relevant
     * property with <code>@SpaceIndex(SpaceIndexType.NONE)</code>.
     */
    @Deprecated boolean inheritIndexes() default PojoDefaults.INHERIT_INDEXES;


    /**
     * When 'get' field method  is not declared with SpaceProperty annotation it is taking into
     * account as space field only if fieldInclusion is IncludeProperties.IMPLICIT. The default is
     * IncludeProperties.IMPLICIT to take into account all the POJO fields.
     */
    IncludeProperties includeProperties() default IncludeProperties.IMPLICIT;

    /**
     * @since 9.0.0
     */
    StorageType storageType() default StorageType.DEFAULT;

    /**
     * When cache policy is "BLOB_STORE" only the indices + minimal administrative information will
     * be kept in cache per entry, while the data will be kept as blobs. This attribute when set to
     * "false" disables blob-store of classes and keeping the entries entirely  cached.
     *
     * @return true if blobStore will be enabled
     */

    boolean blobstoreEnabled() default PojoDefaults.BLOBSTORE_ENABLED;

    /**
     * When partitioned is true, entries will distributed across partitions
     * when false entries will be replicated in each partition
     *
     * @return boolean true for partitioned.
     */

    boolean partitioned() default PojoDefaults.PARTITIONED;
}
