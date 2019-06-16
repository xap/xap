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


package org.openspaces.events.config;

import org.openspaces.events.notify.SimpleNotifyEventListenerContainer;
import org.springframework.beans.factory.support.BeanDefinitionBuilder;
import org.springframework.beans.factory.xml.ParserContext;
import org.springframework.util.StringUtils;
import org.springframework.util.xml.DomUtils;
import org.w3c.dom.Element;

/**
 * @author kimchy
 */
public class NotifyContainerBeanDefinitionParser extends AbstractTemplateEventContainerBeanDefinitionParser {

    private static final String FIFO = "fifo";

    private static final String GUARANTEED = "guaranteed";

    private static final String DURABLE = "durable";

    private static final String TAG = "tag";

    private static final String TRIGGER_NOTIFY_TEMPLATE = "trigger-notify-template";

    private static final String REPLICATE_NOTIFY_TEMPLATE = "replicate-notify-template";

    private static final String PERFORM_TAKE_ON_NOTIFY = "perform-take-on-notify";

    private static final String IGNORE_EVENT_ON_NULL_TAKE = "ignore-event-on-null-take";

    private static final String BATCH = "batch";

    private static final String BATCH_TIME = "time";

    private static final String BATCH_SIZE = "size";

    private static final String BATCH_PENDING_THRESHOLD = "pending-threshold";

    private static final String LEASE = "lease";

    private static final String LEASE_AUTO_RENEW = "auto-renew";

    private static final String LEASE_TIMEOUT = "timeout";

    private static final String NOTIFY_FILTER = "notify-filter";

    private static final String NOTIFY = "notify";

    private static final String PERFORM_SNAPSHOT = "perform-snapshot";

    private static final String PASS_ARRAY_AS_IS = "pass-array-as-is";

    @Override
    protected Class<SimpleNotifyEventListenerContainer> getBeanClass(Element element) {
        return SimpleNotifyEventListenerContainer.class;
    }

    @Override
    protected boolean isSupportsDynamicTemplate() {
        return false;
    }

    @Override
    protected void doParse(Element element, ParserContext parserContext, BeanDefinitionBuilder builder) {

        super.doParse(element, parserContext, builder);

        Element notifyEle = DomUtils.getChildElementByTagName(element, NOTIFY);
        if (notifyEle != null) {
            String write = notifyEle.getAttribute("write");
            if (StringUtils.hasLength(write)) {
                builder.addPropertyValue("notifyWrite", write);
            }
            String update = notifyEle.getAttribute("update");
            if (StringUtils.hasLength(update)) {
                builder.addPropertyValue("notifyUpdate", update);
            }
            String take = notifyEle.getAttribute("take");
            if (StringUtils.hasLength(take)) {
                builder.addPropertyValue("notifyTake", take);
            }
            String leaseExpire = notifyEle.getAttribute("lease-expire");
            if (StringUtils.hasLength(leaseExpire)) {
                builder.addPropertyValue("notifyLeaseExpire", leaseExpire);
            }
            String unmatched = notifyEle.getAttribute("unmatched");
            if (StringUtils.hasLength(unmatched)) {
                builder.addPropertyValue("notifyUnmatched", unmatched);
            }
            String matched = notifyEle.getAttribute("matched-update");
            if (StringUtils.hasLength(matched)) {
                builder.addPropertyValue("notifyMatchedUpdate", matched);
            }
            String rematched = notifyEle.getAttribute("rematched-update");
            if (StringUtils.hasLength(rematched)) {
                builder.addPropertyValue("notifyRematchedUpdate", rematched);
            }
            String all = notifyEle.getAttribute("all");
            if (StringUtils.hasLength(all)) {
                builder.addPropertyValue("notifyAll", all);
            }
        }

        Element batchEle = DomUtils.getChildElementByTagName(element, BATCH);
        if (batchEle != null) {
            builder.addPropertyValue("batchTime", batchEle.getAttribute(BATCH_TIME));
            builder.addPropertyValue("batchSize", batchEle.getAttribute(BATCH_SIZE));
            if (batchEle.hasAttribute(BATCH_PENDING_THRESHOLD)) {
                builder.addPropertyValue("batchPendingThreshold", batchEle.getAttribute(BATCH_PENDING_THRESHOLD));
            }
        }

        Element leaseEle = DomUtils.getChildElementByTagName(element, LEASE);
        if (leaseEle != null) {
            String autoRenew = leaseEle.getAttribute(LEASE_AUTO_RENEW);
            if (StringUtils.hasLength(autoRenew)) {
                builder.addPropertyValue("autoRenew", autoRenew);
            }
            String timeout = leaseEle.getAttribute(LEASE_TIMEOUT);
            if (StringUtils.hasLength(timeout)) {
                builder.addPropertyValue("listenerLease", timeout);
            }
            String listener = leaseEle.getAttribute("listener");
            if (StringUtils.hasLength(listener)) {
                builder.addPropertyReference("leaseListener", listener);
            }
        }

        Element notifyFilterEle = DomUtils.getChildElementByTagName(element, NOTIFY_FILTER);
        if (notifyFilterEle != null) {
            builder.addPropertyValue("notifyFilter", parserContext.getDelegate().parsePropertySubElement(
                    notifyFilterEle, builder.getRawBeanDefinition(), "notifyFilter"));
        }

        String fifo = element.getAttribute(FIFO);
        if (StringUtils.hasLength(fifo)) {
            builder.addPropertyValue("fifo", fifo);
        }

        String guaranteed = element.getAttribute(GUARANTEED);
        if (StringUtils.hasLength(guaranteed)) {
            builder.addPropertyValue("guaranteed", guaranteed);
        }

        String durable = element.getAttribute(DURABLE);
        if (StringUtils.hasLength(durable)) {
            builder.addPropertyValue("durable", durable);
        }

        String tag = element.getAttribute(TAG);
        if (StringUtils.hasLength(tag)) {
            builder.addPropertyValue("tag", tag);
        }

        String triggerNotifyTemplate = element.getAttribute(TRIGGER_NOTIFY_TEMPLATE);
        if (StringUtils.hasLength(triggerNotifyTemplate)) {
            builder.addPropertyValue("triggerNotifyTemplate", triggerNotifyTemplate);
        }

        String replicateNotifyTemplate = element.getAttribute(REPLICATE_NOTIFY_TEMPLATE);
        if (StringUtils.hasLength(replicateNotifyTemplate)) {
            builder.addPropertyValue("replicateNotifyTemplate", replicateNotifyTemplate);
        }

        String performTakeOnNotify = element.getAttribute(PERFORM_TAKE_ON_NOTIFY);
        if (StringUtils.hasLength(performTakeOnNotify)) {
            builder.addPropertyValue("performTakeOnNotify", performTakeOnNotify);
        }

        String ignoreEventOnNullTake = element.getAttribute(IGNORE_EVENT_ON_NULL_TAKE);
        if (StringUtils.hasLength(ignoreEventOnNullTake)) {
            builder.addPropertyValue("ignoreEventOnNullTake", ignoreEventOnNullTake);
        }

        String performSnapshot = element.getAttribute(PERFORM_SNAPSHOT);
        if (StringUtils.hasLength(performSnapshot)) {
            builder.addPropertyValue("performSnapshot", performSnapshot);
        }

        String passArrayAsIs = element.getAttribute(PASS_ARRAY_AS_IS);
        if (StringUtils.hasLength(passArrayAsIs)) {
            builder.addPropertyValue("passArrayAsIs", passArrayAsIs);
        }
    }
}