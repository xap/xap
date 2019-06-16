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


package org.openspaces.events.notify.config;

import com.j_spaces.core.client.INotifyDelegatorFilter;

import net.jini.lease.LeaseListener;

import org.openspaces.core.GigaSpace;
import org.openspaces.core.util.AnnotationUtils;
import org.openspaces.events.SpaceDataEventListener;
import org.openspaces.events.TransactionalEvent;
import org.openspaces.events.notify.Notify;
import org.openspaces.events.notify.NotifyBatch;
import org.openspaces.events.notify.NotifyLease;
import org.openspaces.events.notify.NotifyType;
import org.openspaces.events.notify.ReplicateNotifyTemplateType;
import org.openspaces.events.notify.SimpleNotifyContainerConfigurer;
import org.openspaces.events.notify.TriggerNotifyTemplateType;
import org.openspaces.events.support.AnnotationProcessorUtils;
import org.openspaces.events.support.EventContainersBus;
import org.springframework.aop.support.AopUtils;
import org.springframework.beans.BeansException;
import org.springframework.beans.factory.config.BeanPostProcessor;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.transaction.TransactionDefinition;
import org.springframework.transaction.annotation.Isolation;
import org.springframework.transaction.annotation.Transactional;

/**
 * A {@link org.openspaces.events.notify.Notify} annotation post processor. Creates an internal
 * instance of {@link org.openspaces.events.notify.SimpleNotifyEventListenerContainer} that wraps
 * the given bean (if annotated) listener.
 *
 * @author kimchy
 */
public class NotifyAnnotationPostProcessor implements BeanPostProcessor, ApplicationContextAware {

    private ApplicationContext applicationContext;

    public void setApplicationContext(ApplicationContext applicationContext) throws BeansException {
        this.applicationContext = applicationContext;
    }

    public Object postProcessBeforeInitialization(Object bean, String beanName) throws BeansException {
        return bean;
    }

    public Object postProcessAfterInitialization(final Object bean, String beanName) throws BeansException {
        if (bean == null) {
            return bean;
        }
        Class<?> beanClass = this.getBeanClass(bean);
        if (beanClass == null) {
            return bean;
        }

        Notify notify = AnnotationUtils.findAnnotation(beanClass, Notify.class);
        if (notify == null) {
            return bean;
        }


        GigaSpace gigaSpace = AnnotationProcessorUtils.findGigaSpace(bean, notify.gigaSpace(), applicationContext, beanName);

        EventContainersBus eventContainersBus = AnnotationProcessorUtils.findBus(applicationContext);

        SimpleNotifyContainerConfigurer notifyContainerConfigurer = new SimpleNotifyContainerConfigurer(gigaSpace);

        notifyContainerConfigurer.name(beanName);

        if (bean instanceof SpaceDataEventListener) {
            notifyContainerConfigurer.eventListener((SpaceDataEventListener) bean);
        } else {
            notifyContainerConfigurer.eventListenerAnnotation(bean);
        }
        notifyContainerConfigurer.performSnapshot(notify.performSnapshot());

        notifyContainerConfigurer.ignoreEventOnNullTake(notify.ignoreEventOnNullTake());
        notifyContainerConfigurer.performTakeOnNotify(notify.performTakeOnNotify());

        notifyContainerConfigurer.guaranteed(notify.guaranteed());

        notifyContainerConfigurer.durable(notify.durable());

        if (!notify.tag().isEmpty()) {
            notifyContainerConfigurer.tag(notify.tag());
        }

        notifyContainerConfigurer.fifo(notify.fifo());
        notifyContainerConfigurer.passArrayAsIs(notify.passArrayAsIs());

        notifyContainerConfigurer.autoStart(notify.autoStart());

        if (notify.replicateNotifyTemplate() != ReplicateNotifyTemplateType.DEFAULT) {
            notifyContainerConfigurer.replicateNotifyTemplate(notify.replicateNotifyTemplate() == ReplicateNotifyTemplateType.TRUE);
        }

        if (notify.triggerNotifyTemplate() != TriggerNotifyTemplateType.DEFAULT) {
            notifyContainerConfigurer.triggerNotifyTemplate(notify.triggerNotifyTemplate() == TriggerNotifyTemplateType.TRUE);
        }

        if (!INotifyDelegatorFilter.class.equals(notify.notifyFilter())) {
            try {
                INotifyDelegatorFilter filter = notify.notifyFilter().newInstance();
                notifyContainerConfigurer.notifyFilter(filter);
            } catch (Exception e) {
                throw new IllegalArgumentException("Failed to create [" + notify.notifyFilter() + "]", e);
            }
        }

        // handle transactions (we support using either @Transactional or @TransactionalEvent or both)
        TransactionalEvent transactionalEvent = AnnotationUtils.findAnnotation(beanClass, TransactionalEvent.class);
        Transactional transactional = AnnotationUtils.findAnnotation(beanClass, Transactional.class);
        if (transactionalEvent != null || transactional != null) {
            if (transactionalEvent != null) {
                notifyContainerConfigurer.transactionManager(AnnotationProcessorUtils.findTxManager(transactionalEvent.transactionManager(), applicationContext, beanName));
            } else {
                notifyContainerConfigurer.transactionManager(AnnotationProcessorUtils.findTxManager("", applicationContext, beanName));
            }
            Isolation isolation = Isolation.DEFAULT;
            if (transactional != null && transactional.isolation() != Isolation.DEFAULT) {
                isolation = transactional.isolation();
            }
            if (transactionalEvent != null && transactionalEvent.isolation() != Isolation.DEFAULT) {
                isolation = transactionalEvent.isolation();
            }
            notifyContainerConfigurer.transactionIsolationLevel(isolation.value());

            int timeout = TransactionDefinition.TIMEOUT_DEFAULT;
            if (transactional != null && transactional.timeout() != TransactionDefinition.TIMEOUT_DEFAULT) {
                timeout = transactional.timeout();
            }
            if (transactionalEvent != null && transactionalEvent.timeout() != TransactionDefinition.TIMEOUT_DEFAULT) {
                timeout = transactionalEvent.timeout();
            }
            notifyContainerConfigurer.transactionTimeout(timeout);
        }

        NotifyType notifyType = AnnotationUtils.findAnnotation(beanClass, NotifyType.class);
        if (notifyType != null) {
            notifyContainerConfigurer.notifyWrite(notifyType.write());
            notifyContainerConfigurer.notifyUpdate(notifyType.update());
            notifyContainerConfigurer.notifyTake(notifyType.take());
            notifyContainerConfigurer.notifyLeaseExpire(notifyType.leaseExpire());
            notifyContainerConfigurer.notifyUnmatched(notifyType.unmatched());
            notifyContainerConfigurer.notifyMatchedUpdate(notifyType.matchedUpdate());
            notifyContainerConfigurer.notifyRematchedUpdate(notifyType.rematchedUpdate());

        }

        NotifyBatch notifyBatch = AnnotationUtils.findAnnotation(beanClass, NotifyBatch.class);
        if (notifyBatch != null) {
            notifyContainerConfigurer.batchSize(notifyBatch.size());
            notifyContainerConfigurer.batchTime(notifyBatch.time());
            notifyContainerConfigurer.batchPendingThreshold(notifyBatch.pendingThreshold());
            notifyContainerConfigurer.passArrayAsIs(notifyBatch.passArrayAsIs());
        }

        NotifyLease notifyLease = AnnotationUtils.findAnnotation(beanClass, NotifyLease.class);
        if (notifyLease != null) {
            notifyContainerConfigurer.autoRenew(true);
            notifyContainerConfigurer.listenerLease(notifyLease.lease());
            notifyContainerConfigurer.renewExpiration(notifyLease.renewExpiration());
            notifyContainerConfigurer.renewDuration(notifyLease.renewDuration());
            notifyContainerConfigurer.renewRTT(notifyLease.renewRTT());
            if (!LeaseListener.class.equals(notifyLease.leaseListener())) {
                try {
                    LeaseListener leaseListener = notifyLease.leaseListener().newInstance();
                    notifyContainerConfigurer.leaseListener(leaseListener);
                } catch (Exception e) {
                    throw new IllegalArgumentException("Failed to create [" + notifyLease.leaseListener() + "]", e);
                }
            }
        }

        eventContainersBus.registerContainer(beanName, notifyContainerConfigurer.notifyContainer());

        return bean;
    }

    private Class<?> getBeanClass(Object bean) {
        return AopUtils.getTargetClass(bean);
    }
}