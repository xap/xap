/*
 * Copyright 2005 Sun Microsystems, Inc.
 * Copyright 2005 GigaSpaces, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *      http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.jini.rio.resources.resource;

import java.util.ArrayList;
import java.util.Hashtable;
import java.util.LinkedList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A generic abstract class to handle a finite set of resources
 */
public abstract class ResourcePool {
    /**
     * The collection of resource pools
     */
    static final Hashtable theInstances = new Hashtable();
    /**
     * The free list
     */
    protected LinkedList freed = new LinkedList();
    /**
     * The alloced list
     */
    protected LinkedList alloced = new LinkedList();
    /**
     * The minimum # of resources to create at init time
     */
    protected int min;
    /**
     * The maximum # of resources allowed
     */
    protected int max = Integer.MAX_VALUE;
    /**
     * The total number of resources currently created
     */
    protected int numCreated;
    /**
     * Unique identifier for a resourcePool instance
     */
    protected String identifier;
    /**
     * A Logger
     */
    static Logger logger = LoggerFactory.getLogger("org.jini.rio.resources.resource");

    /**
     * Abstract method for creating a specific resource in the sub-class
     *
     * @return the resource
     * @throws ResourceUnavailableException if an error occured in creating the resource
     */
    protected abstract Object create() throws ResourceUnavailableException;

    /**
     * Abstract method for disposing a specific resource in the sub-class
     *
     * @param obj the resource to dispose
     */
    protected abstract void dispose(Object obj);

    /**
     * Abstract method for validating a resource
     *
     * @param obj the resource to validate
     * @return true if resource is still valid
     */
    protected abstract boolean validate(Object obj);

    /**
     * Release a resource back to the pool after its use is completed.
     *
     * @param obj the resource object
     */
    public synchronized void release(Object obj) {
        if (obj == null) {
            throw new NullPointerException("ResourcePool: managed resource "
                    + "cannot be null");
        }
        if (alloced.remove(obj) == false) {
            throw new IllegalArgumentException("ResourcePool: unknown resource "
                    + obj);
        }
        freeResource(obj);
        notifyAll();
    }

    /**
     * Remove a resource all together from the pool and dispose of it
     *
     * @param obj the resource object
     */
    public synchronized void remove(Object obj) {
        if (alloced.remove(obj) || freed.remove(obj))
            numCreated--;
        disposeResource(obj);
    }

    /**
     * Get a Resource from the pool, blocking forever until a resource becomes available
     *
     * @return the resource object
     * @throws ResourceUnavailableException if an error occured in creating the resource
     */
    public synchronized Object get() throws ResourceUnavailableException {
        return (get(0));
    }

    /**
     * Get a Resource from the pool, blocking for a specified time until a resource becomes
     * available, a zero timeout blocks forever.
     *
     * @param timeout the time in milliseconds to wait for a resource to become available
     * @return the resource object
     * @throws ResourceUnavailableException if an error occured in creating the resource
     * @throws ResourceUnavailableException if no resource becomes available before the timeout
     *                                      period expires
     */
    public synchronized Object get(long timeout)
            throws ResourceUnavailableException {
        while (true) {
            // if we have reached the max,
            // then wait for up to timeout milliseconds, (or forever if zero)
            // until some one frees one
            if (numCreated >= max) {
                while (freed.size() == 0) {
                    try {
                        wait(timeout);
                    } catch (InterruptedException ignore) {
                        logger.warn("Getting a resource has been interrupted");
                    }
                    if (timeout > 0)
                        break;
                }
            }
            // if there are no resources on the free list and
            // if the maximum has not been reached, create a new one.
            // otherwise throw the ResourceUnavailableException exception
            if (freed.size() == 0) {
                if (numCreated >= max) {
                    throw new ResourceUnavailableException("Resource: "
                            + this.getClass().getName()
                            + " is not available after "
                            + timeout
                            + " milliseconds");
                }
                createResource();
            }
            // if we get this far there should be at least one resource on the
            // free list. Get the first one and validate it.
            Object obj = freed.removeFirst();
            if (obj == null)
                throw new ResourceUnavailableException("Resource: "
                        + this.getClass().getName()
                        + "is corrupted");
            if (validate(obj)) { // good, so return it
                allocResource(obj);
                return (obj);
            }
            // if we get this far the object is bad, so throw it away
            try {
                numCreated--;
                disposeResource(obj);
            } catch (Exception e) {
                logger.warn("Disposing a resource", e);
            }
        }
    }

    /**
     * creates a new Resource and performs any initilization required to manage the resource
     *
     * @return the new resource
     */
    protected Object createResource() throws ResourceUnavailableException {
        Object resource = create();
        freed.add(resource);
        numCreated++;
        return (resource);
    }

    /**
     * allocates a Resource and performs any initilization required to manage the alloced resource
     *
     * @param resource the resource to allocate
     */
    protected void allocResource(Object resource) {
        alloced.add(resource);
    }

    /**
     * frees a Resource and performs any clean-up required to manage the resource
     *
     * @param resource the resource to free
     */
    protected void freeResource(Object resource) {
        freed.add(resource);
    }

    /**
     * disposes of a Resource and performs any clean-up required to manage the resource
     *
     * @param resource the resource to dispose
     */
    protected void disposeResource(Object resource) {
        dispose(resource);
    }

    /**
     * Gets the number of resources currently available for assignment
     *
     * @return the number of resources currently available for assignment
     */
    public synchronized int getFreedSize() {
        return (freed.size());
    }

    /**
     * Gets the number of resources currently assigned
     *
     * @return the number of resources currently assigned
     */
    public synchronized int getAllocedSize() {
        return (alloced.size());
    }

    @SuppressWarnings("unchecked")
    public synchronized <T> List<T> getAllocedOfType(Class<T> type) {
        List<T> ret = new ArrayList<T>();
        for (Object obj : alloced) {
            if (type.isInstance(type)) {
                ret.add((T) obj);
            }
        }
        return ret;
    }

    /**
     * Gets the total number of resources created
     *
     * @return the total number of resources created
     */
    public synchronized int getSize() {
        return (numCreated);
    }

    /**
     * Gets the minimum number of resources
     *
     * @return the minimum number of resources
     */
    public int getMin() {
        return (min);
    }

    /**
     * Gets the maximum number of resources allowed
     *
     * @return the maximum number of resources allowed
     */
    public int getMax() {
        return (max);
    }

    /**
     * Sets the minimum number of resources, this method has no effect on the number of created
     * resources, if the new minimum is less than the old minimum.
     *
     * @param min the minimum number of resources
     * @throws IllegalArgumentException if minimum is less than zero or the minimum is is greater
     *                                  than the maximum
     */
    public synchronized void setMin(int min) {
        if (min < 0 || min > this.max)
            throw new IllegalArgumentException("ResourcePool: min > max resources");
        this.min = min;
    }

    /**
     * Sets the unique identifier for this instance
     *
     * @param identifier the unique identifier for this instance
     */
    public void setIdentifier(String identifier) {
        if (identifier == null)
            throw new NullPointerException("identifier is null");
        this.identifier = identifier;
    }

    /**
     * Gets the unique identifier for this instance
     *
     * @return the unique identifier for this instance
     */
    public String getIdentifier() {
        return (this.identifier);
    }

    /**
     * Pre-creates the minimum number of resources
     */
    protected void createResources() throws ResourceUnavailableException {
        createResources(this.min);
    }

    /**
     * Pre-creates the specified number of resources
     *
     * @param number the number of resources to create
     */
    protected void createResources(int number)
            throws ResourceUnavailableException {
        while (numCreated < number)
            createResource();
    }

    /**
     * Sets the maximum number of resources
     *
     * @param max the maximum number of resources
     * @throws IllegalArgumentException if maximum is less than the minimum
     */
    public synchronized void setMax(int max) {
        if (max < this.min)
            throw new IllegalArgumentException("ResourcePool: min > max resources");
        this.max = max;
    }

    /**
     * Gets a registered ResourcePool
     *
     * @param key the identifier for the type of resource pool
     * @return the resource pool or null, if one is not registered for the key.
     */
    public static ResourcePool getInstance(String key) {
        return (ResourcePool) theInstances.get(key);
    }

    /**
     * Registers this resource into the common ResourcePool
     */
    protected void register() {
        register(identifier, this);
    }

    /**
     * Registers this resource into the common ResourcePool
     *
     * @param key the identifier for the type of resource pool
     */
    protected void register(String key) {
        register(key, this);
    }

    /**
     * Registers a resource into the common ResourcePool
     *
     * @param key  the identifier for the type of resource pool
     * @param pool the ResourcePool being registered
     */
    public static void register(String key, ResourcePool pool) {
        theInstances.put(key, pool);
    }

    /**
     * Returns a string representation of the ResourcePool
     *
     * @return a string representation of the ResourcePool
     */
    public String toString() {
        return (identifier
                + ":min="
                + getMin()
                + ":max="
                + getMax()
                + ":create="
                + getSize()
                + ":free="
                + getFreedSize()
                + ":asgn="
                + getAllocedSize()
                + ":audit=" + (getSize() - (getAllocedSize() + getFreedSize())));
    }
}
