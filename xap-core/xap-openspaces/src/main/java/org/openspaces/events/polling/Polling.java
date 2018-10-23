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


package org.openspaces.events.polling;

import org.springframework.stereotype.Component;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Marks an event listener as polled event listener. It will be wrapped automatically with {@link
 * org.openspaces.events.polling.SimplePollingEventListenerContainer}.
 *
 * <p>Template can be provided using {@link org.openspaces.events.EventTemplate} marked on a general
 * method that returns the template.
 *
 * <p>The event listener method should be marked with {@link org.openspaces.events.adapter.SpaceDataEvent}.
 *
 * @author kimchy
 * @see org.openspaces.events.TransactionalEvent
 * @see org.openspaces.events.polling.ReceiveHandler
 * @see org.openspaces.events.polling.TriggerHandler
 */
@Target(ElementType.TYPE)
@Retention(RetentionPolicy.RUNTIME)
@Component
public @interface Polling {

    /**
     * The value may indicate a suggestion for a logical component name, to be turned into a Spring
     * bean in case of an autodetected component.
     *
     * @return the suggested component name, if any
     */
    String value() default "";

    /**
     * The name of the bean that is the {@link org.openspaces.core.GigaSpace} this container will
     * use.
     *
     * <p>Note, this is optional. If there is only one {@link org.openspaces.core.GigaSpace} defined
     * in the application context, it will be used.
     */
    String gigaSpace() default "";

    /**
     * Specify the number of concurrent consumers to create. Default is 1.
     *
     * <p> Specifying a higher value for this setting will increase the standard level of scheduled
     * concurrent consumers at runtime: This is effectively the minimum number of concurrent
     * consumers which will be scheduled at any given time. This is a static setting; for dynamic
     * scaling, consider specifying the "maxConcurrentConsumers" setting instead.
     *
     * <p> Raising the number of concurrent consumers is recommended in order to scale the
     * consumption of events. However, note that any ordering guarantees are lost once multiple
     * consumers are registered. In general, stick with 1 consumer for low-volume events.
     *
     * @see org.openspaces.events.polling.SimplePollingEventListenerContainer#setConcurrentConsumers(int)
     */
    int concurrentConsumers() default 1;

    /**
     * Specify the maximum number of concurrent consumers to create. Default is 1.
     *
     * <p> If this setting is higher than "concurrentConsumers", the listener container will
     * dynamically schedule new consumers at runtime, provided that enough incoming messages are
     * encountered. Once the load goes down again, the number of consumers will be reduced to the
     * standard level ("concurrentConsumers") again.
     *
     * <p> Raising the number of concurrent consumers is recommended in order to scale the
     * consumption of events. However, note that any ordering guarantees are lost once multiple
     * consumers are registered. In general, stick with 1 consumer for low-volume events.
     *
     * @see org.openspaces.events.polling.SimplePollingEventListenerContainer#setMaxConcurrentConsumers(int)
     */
    int maxConcurrentConsumers() default 1;

    /**
     * Set the timeout to use for receive calls, in <b>milliseconds</b>. The default is 60000 ms,
     * that is, 1 minute.
     *
     * <p><b>NOTE:</b> This value needs to be smaller than the transaction timeout used by the
     * transaction manager (in the appropriate unit, of course).
     *
     * @see org.openspaces.events.polling.SimplePollingEventListenerContainer#setReceiveTimeout(long)
     */
    long receiveTimeout() default SimplePollingEventListenerContainer.DEFAULT_RECEIVE_TIMEOUT;

    /**
     * If set to <code>true</code> will perform prepareTemplate operation on the provided template before
     * invoking registering as an event listener.
     *
     * @see org.openspaces.core.GigaSpace#prepareTemplate(Object)
     * @see org.openspaces.events.polling.SimplePollingEventListenerContainer#setPerformSnapshot(boolean)
     */
    boolean performSnapshot() default true;

    /**
     * Set whether this container will start once instantiated.
     *
     * <p>Default is <code>true</code>. Set to <code>false</code> in order for this container to be
     * started using {@link SimplePollingEventListenerContainer#start()}.
     */
    boolean autoStart() default true;

    /**
     * If set to <code>true</code> will pass an array value returned from a {@link
     * org.openspaces.events.polling.receive.ReceiveOperationHandler} directly to the listener
     * without "serializing" it as one array element each time. Defaults to <code>false</code>
     *
     * @see org.openspaces.events.polling.SimplePollingEventListenerContainer#setPassArrayAsIs(boolean)
     */
    boolean passArrayAsIs() default false;

    /**
     * @see org.openspaces.events.polling.SimplePollingEventListenerContainer#setRecoveryInterval(long)
     */
    long recoveryInterval() default SimplePollingEventListenerContainer.DEFAULT_RECOVERY_INTERVAL;

    /**
     * @see org.openspaces.events.polling.SimplePollingEventListenerContainer#setKeepAliveRetries(int)
     */
    int keepAliveRetries() default SimplePollingEventListenerContainer.DEFAULT_KEEP_ALIVE_RETRIES;
}