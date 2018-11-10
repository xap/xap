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

package com.gigaspaces.internal.server.space;

import com.gigaspaces.internal.backport.java.util.concurrent.FastConcurrentSkipListMap;
import com.gigaspaces.internal.server.storage.ITemplateHolder;
import com.gigaspaces.internal.utils.concurrent.GSThread;
import com.gigaspaces.lrmi.nio.IResponseContext;
import com.gigaspaces.time.SystemTime;
import com.j_spaces.core.AnswerHolder;
import com.j_spaces.core.cache.CacheManager;
import com.j_spaces.core.cache.context.Context;
import com.j_spaces.core.exception.ClosedResourceException;
import com.j_spaces.kernel.IConsumerObject;
import com.j_spaces.kernel.WorkingGroup;
import com.j_spaces.kernel.locks.ILockObject;

import java.util.Iterator;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * a time based service that is responsible of sending back an empty response back to the client
 * when the template timeout is reached and no answer is found.
 *
 * @author asy ronen
 * @since 6.1
 */
@com.gigaspaces.api.InternalApi
public class TemplateExpirationManager implements IConsumerObject<Object> {
    private static final Logger _logger = Logger.getLogger(com.gigaspaces.logger.Constants.LOGGER_ENGINE);

    private static final int minPoolSize = 1;
    private static final int maxPoolSize = 16;
    private static final int timeout = 5 * 60 * 1000;
    private static final int bucketTimeSpan = 10;

    private final CacheManager _cacheManager;
    private final FastConcurrentSkipListMap<TemplateKey, ITemplateHolder> _expirationList;
    private final WorkingGroup<Object> _threadPool;
    private final Timer _timer;

    public TemplateExpirationManager(CacheManager cacheManager) {
        _cacheManager = cacheManager;
        _expirationList = new FastConcurrentSkipListMap<TemplateKey, ITemplateHolder>();
        _threadPool = new WorkingGroup<Object>(this,
                Thread.NORM_PRIORITY, "TemplateExpirationManager",
                minPoolSize, maxPoolSize, timeout);
        _timer = new Timer("GS-Template-Expiration-Manager-Timer");
    }

    public void start() {
        _timer.start();
        _threadPool.start();
    }

    @Override
    public void dispatch(Object req) {
        ITemplateHolder template = null;
        Exception exp = null;
        boolean fromTimerThread = true;
        if ((req instanceof ITemplateHolder)) {
            template = (ITemplateHolder) req;
        } else {
            template = ((ReturnWithExecption) (req)).getTemplate();
            exp = ((ReturnWithExecption) (req)).getException();
            fromTimerThread = false;
        }

        cancelTemplateHolder(template, exp, fromTimerThread);
    }

    public void cancelTemplateHolder(ITemplateHolder template, Exception exp) {
        cancelTemplateHolder(template, exp, false);
    }


    private void cancelTemplateHolder(ITemplateHolder template, Exception exp, boolean fromTimerThread) {
        IResponseContext respContext = template.getResponseContext();
        if (respContext != null || template.getMultipleIdsContext() != null || exp != null) {
            ILockObject templateLock = _cacheManager.getLockManager().getLockObject(template, false /*isEvictable*/);
            Context context = null;
            AnswerHolder aHolder = template.getAnswerHolder();
            try {
                synchronized (templateLock) {

                    synchronized (aHolder) {
                        if (!template.isDeleted()) {
                            if (!fromTimerThread)
                                removeTemplate(template);
                            else  //already removed from list by timer thread
                                template.setInExpirationManager(false);
                            context = _cacheManager.getCacheContext();
                            context.setOperationAnswer(template, null, exp);
                            _cacheManager.removeTemplate(context, template, false, true /*origin*/, false);
                        } else {
                            return;
                        }
                    }
                }//synchronized (templateLock)
            } catch (ClosedResourceException ex) {
                _logger.log(Level.FINE, "exception occurred during template removing", ex);
            } finally {
                if (templateLock != null)
                    _cacheManager.getLockManager().freeLockObject(templateLock);
                _cacheManager.freeCacheContext(context);
            }

        }
    }

    @Override
    public void cleanUp() {
    }

    public void shutDown() {
        _expirationList.clear();
        _timer.close();
        _threadPool.shutdown();
    }

    private static class TemplateKey implements Comparable {
        private Long _timestamp;
        private String _id;
        private ITemplateHolder _template;

        private TemplateKey(long expirationTime, String templateID, ITemplateHolder template) {
            this._timestamp = expirationTime;
            this._id = templateID;
            this._template = template;
        }

        public int compareTo(Object o) {
            TemplateKey other = (TemplateKey) o;
            int firstRes = this._timestamp.compareTo(other._timestamp);
            if (firstRes != 0)
                return firstRes;

            return this._id.compareTo(other._id);
        }

        @Override
        public boolean equals(Object obj) {
            TemplateKey other = (TemplateKey) obj;
            return (other._template == this._template);
        }

        @Override
        public int hashCode() {
            return _timestamp.hashCode() + 17 * _id.hashCode();
        }
    }

    private static class ReturnWithExecption {
        private final ITemplateHolder _template;
        private final Exception _exception;

        private ReturnWithExecption(ITemplateHolder template, Exception exception) {
            _template = template;
            _exception = exception;
        }

        ITemplateHolder getTemplate() {
            return _template;
        }

        Exception getException() {
            return _exception;
        }
    }

    public void addTemplate(ITemplateHolder template) {
        if ((template.getResponseContext() == null && template.getMultipleIdsContext() == null) ||
                template.getExpirationTime() == Long.MAX_VALUE ||
                template.isNotifyTemplate()) {
            return;
        }
        template.setInExpirationManager(true);
        TemplateKey mykey = createTemplateKey(template);

        _expirationList.put(mykey, template);
        _timer.updateTimeIfNeccesary(mykey._timestamp);

        //is quiesce mode set? prevent a situation in which q mode set and the loop of disabling pending templates
        //already finished
        try {
            _cacheManager.getEngine().getSpaceImpl().getQuiesceHandler().checkAllowedOp(template.getSpaceContext() != null ? template.getSpaceContext().getQuiesceToken() : null);
        } catch (Exception ex) {
            _threadPool.enqueueBlocked(new ReturnWithExecption(template, ex));
        }
    }

    //for every pending template return with  exception-
    //currently called when Quiesec mode is activated
    public void returnWithExceptionFromAllPendingTemplates(Exception exp) {
        Iterator<ITemplateHolder> iter = _expirationList.values().iterator();
        while (iter.hasNext()) {
            ITemplateHolder template = iter.next();
            if (!template.isDeleted())
                _threadPool.enqueueBlocked(new ReturnWithExecption(template, exp));
            }
    }

    private static TemplateKey createTemplateKey(ITemplateHolder template) {
        long expTimeStamp = template.getExpirationTime();

        long bucketKey = expTimeStamp + (bucketTimeSpan - (expTimeStamp % bucketTimeSpan));
        return new TemplateKey(bucketKey, template.getUID(), template);
    }

    public void removeTemplate(ITemplateHolder template) {
        if (!template.isInExpirationManager())
            return;
        template.setInExpirationManager(false);
        _expirationList.remove(createTemplateKey(template));
    }


    private class Timer extends GSThread {
        private volatile boolean _closed;
        private final Object _timerObj = new Object();
        private volatile long _nextAwakening = Long.MAX_VALUE;
        private boolean _scanningTheList;

        /**
         * Timer daemon thread.
         *
         * @param threadName the name of this daemon thread.
         */
        public Timer(String threadName) {
            super(threadName);
            this.setDaemon(true);
        }

        public void updateTimeIfNeccesary(long timestamp) {
            long nextAwakening = _nextAwakening;
            if (nextAwakening <= timestamp)
                return; //nothing to do

            synchronized (_timerObj) {
                if (_nextAwakening > timestamp) {
                    _nextAwakening = timestamp;
                    if (!_scanningTheList)
                        _timerObj.notify();
                }
            }
        }

        @Override
        public void run() {
            while (!_closed) {
                boolean hasMoreTemplates = true;
                long currenttime = SystemTime.timeMillis();
                long nextTime = Long.MAX_VALUE;
                while (hasMoreTemplates && !_expirationList.isEmpty()) {
                    try {
                        TemplateKey firstKey = _expirationList.firstKey();
                        if (currenttime >= firstKey._timestamp) {
                            //remove first element
                            Map.Entry entry = _expirationList.pollFirstEntry();
                            if (entry == null) {
                                hasMoreTemplates = false;
                                continue;
                            }
                            firstKey = (TemplateKey) (entry.getKey());
                            if (currenttime < firstKey._timestamp && !firstKey._template.isDeleted()) {
                                //protect against the case that the first template was deleted from the outside and
                                //the next template does not fit for deletion yet
                                _expirationList.put(firstKey, firstKey._template);
                                nextTime = firstKey._timestamp;
                                hasMoreTemplates = false;
                                continue;

                            }
                            ITemplateHolder template = (ITemplateHolder) entry.getValue();
                            if (!template.isDeleted())
                                _threadPool.enqueueBlocked(template);
                        } else {
                            nextTime = firstKey._timestamp;
                            hasMoreTemplates = false;
                        }
                    } catch (NoSuchElementException ex) {
                        hasMoreTemplates = false;
                    }
                }//while (hasMoreTemplates && !_expirationList.isEmpty() )

                boolean needsleep = true;
                synchronized (_timerObj) {//set the waiting interval for next round
                    try {
                        while (needsleep) {
                            _scanningTheList = false;
                            _nextAwakening = Math.min(_nextAwakening, nextTime);
                            long timeToSleep = _nextAwakening - SystemTime.timeMillis();
                            if (timeToSleep > 0) {
                                _timerObj.wait(timeToSleep);
                            } else
                                needsleep = false;

                            nextTime = Long.MAX_VALUE;
                        }
                        _scanningTheList = true;
                        _nextAwakening = Long.MAX_VALUE;
                    } catch (InterruptedException e) {
                        break;
                    }
                }
            }
        }

        public void close() {
            _closed = true;
            interrupt();
        }
    }
}
