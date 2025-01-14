/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.dubbo.registry.support;

import org.apache.dubbo.common.URL;
import org.apache.dubbo.common.timer.HashedWheelTimer;
import org.apache.dubbo.common.utils.CollectionUtils;
import org.apache.dubbo.common.utils.NamedThreadFactory;
import org.apache.dubbo.registry.NotifyListener;
import org.apache.dubbo.registry.retry.FailedNotifiedTask;
import org.apache.dubbo.registry.retry.FailedRegisteredTask;
import org.apache.dubbo.registry.retry.FailedSubscribedTask;
import org.apache.dubbo.registry.retry.FailedUnregisteredTask;
import org.apache.dubbo.registry.retry.FailedUnsubscribedTask;
import org.apache.dubbo.remoting.Constants;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;

import static org.apache.dubbo.common.constants.CommonConstants.FILE_KEY;
import static org.apache.dubbo.registry.Constants.CONSUMER_PROTOCOL;
import static org.apache.dubbo.registry.Constants.DEFAULT_REGISTRY_RETRY_PERIOD;
import static org.apache.dubbo.registry.Constants.REGISTRY_RETRY_PERIOD_KEY;

/**
 * 失败重试注册中心，主要是扩展了失败重试的策略
 * FailbackRegistry. (SPI, Prototype, ThreadSafe)
 */
public abstract class FailbackRegistry extends AbstractRegistry {

    /*  retry task map */

    /**
     * 注册失败的URL和定时器集合
     */
    private final ConcurrentMap<URL, FailedRegisteredTask> failedRegistered = new ConcurrentHashMap<URL, FailedRegisteredTask>();

    /**
     * 取消注册失败的URL和定时器集合
     */
    private final ConcurrentMap<URL, FailedUnregisteredTask> failedUnregistered = new ConcurrentHashMap<URL, FailedUnregisteredTask>();

    /**
     * 订阅失败的集合
     */
    private final ConcurrentMap<Holder, FailedSubscribedTask> failedSubscribed = new ConcurrentHashMap<Holder, FailedSubscribedTask>();

    /**
     * 取消订阅失败的集合
     */
    private final ConcurrentMap<Holder, FailedUnsubscribedTask> failedUnsubscribed = new ConcurrentHashMap<Holder, FailedUnsubscribedTask>();

    /**
     * 通知失败的集合
     */
    private final ConcurrentMap<Holder, FailedNotifiedTask> failedNotified = new ConcurrentHashMap<Holder, FailedNotifiedTask>();

    /**
     * 重试间隔时间毫秒
     * The time in milliseconds the retryExecutor will wait
     */
    private final int retryPeriod;

    //    失败重试定时器，定时去检查是否有请求失败的，如有，无限次重试。
    // Timer for failure retry, regular check if there is a request for failure, and if there is, an unlimited retry
    private final HashedWheelTimer retryTimer;

    /**
     * 构造器
     *
     * @param url
     */
    public FailbackRegistry(URL url) {
        //调用父类，设置注册中心，本地缓存等
        super(url);

        //从请求中根据retry.period获取重试的频率， 默认为5秒
        this.retryPeriod = url.getParameter(REGISTRY_RETRY_PERIOD_KEY, DEFAULT_REGISTRY_RETRY_PERIOD);

        // since the retry task will not be very much. 128 ticks is enough.
        //创建重试定时器
        retryTimer = new HashedWheelTimer(new NamedThreadFactory("DubboRegistryRetryTimer", true), retryPeriod,
            TimeUnit.MILLISECONDS, 128);
    }

    /**
     * 在注册失败的集合中将指定的值移除
     *
     * @param url
     */
    public void removeFailedRegisteredTask(URL url) {

        failedRegistered.remove(url);
    }

    /**
     * 在取消注册失败的集合中将指定的值移除
     *
     * @param url
     */
    public void removeFailedUnregisteredTask(URL url) {

        failedUnregistered.remove(url);
    }

    /**
     * 在订阅失败的集合中将指定的值移除
     *
     * @param url
     * @param listener
     */
    public void removeFailedSubscribedTask(URL url, NotifyListener listener) {

        Holder h = new Holder(url, listener);
        failedSubscribed.remove(h);
    }

    /**
     * 在取消订阅失败的集合中将指定的值移除
     *
     * @param url
     * @param listener
     */
    public void removeFailedUnsubscribedTask(URL url, NotifyListener listener) {

        Holder h = new Holder(url, listener);
        failedUnsubscribed.remove(h);
    }

    /**
     * 在通知失败的集合中将指定的值移除
     *
     * @param url
     * @param listener
     */
    public void removeFailedNotifiedTask(URL url, NotifyListener listener) {

        Holder h = new Holder(url, listener);
        failedNotified.remove(h);
    }

    /**
     * 将指定的URL加入失败注册集合中，并新增一个定时任务
     *
     * @param url
     */
    private void addFailedRegistered(URL url) {

        FailedRegisteredTask oldOne = failedRegistered.get(url);
        if (oldOne != null) {
            return;
        }
        FailedRegisteredTask newTask = new FailedRegisteredTask(url, this);
        oldOne = failedRegistered.putIfAbsent(url, newTask);
        if (oldOne == null) {
            // never has a retry task. then start a new task for retry.
            retryTimer.newTimeout(newTask, retryPeriod, TimeUnit.MILLISECONDS);
        }
    }

    /**
     * 将指定的URL从注册失败的集合中移除，并将重试的任务取消
     *
     * @param url
     */
    private void removeFailedRegistered(URL url) {

        FailedRegisteredTask f = failedRegistered.remove(url);
        if (f != null) {
            f.cancel();
        }
    }

    /**
     * 添加取消注册失败的任务，并新增一个定时任务
     * @param url
     */
    private void addFailedUnregistered(URL url) {

        FailedUnregisteredTask oldOne = failedUnregistered.get(url);
        if (oldOne != null) {
            return;
        }
        FailedUnregisteredTask newTask = new FailedUnregisteredTask(url, this);
        oldOne = failedUnregistered.putIfAbsent(url, newTask);
        if (oldOne == null) {
            // never has a retry task. then start a new task for retry.
            retryTimer.newTimeout(newTask, retryPeriod, TimeUnit.MILLISECONDS);
        }
    }

    /**
     * 移除取消注册任务，并取消
     * @param url
     */
    private void removeFailedUnregistered(URL url) {

        FailedUnregisteredTask f = failedUnregistered.remove(url);
        if (f != null) {
            f.cancel();
        }
    }

    /**
     * 添加订阅失败任务
     * @param url
     * @param listener
     */
    private void addFailedSubscribed(URL url, NotifyListener listener) {

        Holder h = new Holder(url, listener);
        FailedSubscribedTask oldOne = failedSubscribed.get(h);
        if (oldOne != null) {
            return;
        }
        FailedSubscribedTask newTask = new FailedSubscribedTask(url, this, listener);
        oldOne = failedSubscribed.putIfAbsent(h, newTask);
        if (oldOne == null) {
            // never has a retry task. then start a new task for retry.
            retryTimer.newTimeout(newTask, retryPeriod, TimeUnit.MILLISECONDS);
        }
    }

    /**
     * 移除订阅失败任务并取消
     * @param url
     * @param listener
     */
    private void removeFailedSubscribed(URL url, NotifyListener listener) {

        Holder h = new Holder(url, listener);
        FailedSubscribedTask f = failedSubscribed.remove(h);
        if (f != null) {
            f.cancel();
        }
        removeFailedUnsubscribed(url, listener);
        removeFailedNotified(url, listener);
    }

    /**
     * 添加取消订阅失败任务
     * @param url
     * @param listener
     */
    private void addFailedUnsubscribed(URL url, NotifyListener listener) {

        Holder h = new Holder(url, listener);
        FailedUnsubscribedTask oldOne = failedUnsubscribed.get(h);
        if (oldOne != null) {
            return;
        }
        FailedUnsubscribedTask newTask = new FailedUnsubscribedTask(url, this, listener);
        oldOne = failedUnsubscribed.putIfAbsent(h, newTask);
        if (oldOne == null) {
            // never has a retry task. then start a new task for retry.
            retryTimer.newTimeout(newTask, retryPeriod, TimeUnit.MILLISECONDS);
        }
    }

    /**
     * 移除取消订阅失败任务并取消
     * @param url
     * @param listener
     */
    private void removeFailedUnsubscribed(URL url, NotifyListener listener) {

        Holder h = new Holder(url, listener);
        FailedUnsubscribedTask f = failedUnsubscribed.remove(h);
        if (f != null) {
            f.cancel();
        }
    }

    /**
     * 添加通知失败任务
     * @param url
     * @param listener
     * @param urls
     */
    private void addFailedNotified(URL url, NotifyListener listener, List<URL> urls) {

        Holder h = new Holder(url, listener);
        FailedNotifiedTask newTask = new FailedNotifiedTask(url, listener);
        FailedNotifiedTask f = failedNotified.putIfAbsent(h, newTask);
        if (f == null) {
            // never has a retry task. then start a new task for retry.
            newTask.addUrlToRetry(urls);
            retryTimer.newTimeout(newTask, retryPeriod, TimeUnit.MILLISECONDS);
        } else {
            // just add urls which needs retry.
            newTask.addUrlToRetry(urls);
        }
    }

    /**
     * 移除通知失败任务
     * @param url
     * @param listener
     */
    private void removeFailedNotified(URL url, NotifyListener listener) {

        Holder h = new Holder(url, listener);
        FailedNotifiedTask f = failedNotified.remove(h);
        if (f != null) {
            f.cancel();
        }
    }

    ConcurrentMap<URL, FailedRegisteredTask> getFailedRegistered() {

        return failedRegistered;
    }

    ConcurrentMap<URL, FailedUnregisteredTask> getFailedUnregistered() {

        return failedUnregistered;
    }

    ConcurrentMap<Holder, FailedSubscribedTask> getFailedSubscribed() {

        return failedSubscribed;
    }

    ConcurrentMap<Holder, FailedUnsubscribedTask> getFailedUnsubscribed() {

        return failedUnsubscribed;
    }

    ConcurrentMap<Holder, FailedNotifiedTask> getFailedNotified() {

        return failedNotified;
    }

    /**
     * 注册 重新父类方法
     *
     * @param url
     */
    @Override
    public void register(URL url) {

        //调用父类的注册方法，将URL存入registered中
        super.register(url);

        //将该URL从注册失败的集合中移除，并将正在进行重试的任务取消
        removeFailedRegistered(url);

        //将该URL从取消注册失败的集合中移除，并将正在进行重试的任务取消
        removeFailedUnregistered(url);

        try {
            // Sending a registration request to the server side
            //做注册的操作，模板方法，需要子类实现
            doRegister(url);
        } catch (Exception e) {
            Throwable t = e;

            // If the startup detection is opened, the Exception is thrown directly.
            //            当设置 <dubbo:registry check="false" /> 时，记录失败注册和订阅请求，后台定时重试
            boolean check =
                getUrl().getParameter(Constants.CHECK_KEY, true) && url.getParameter(Constants.CHECK_KEY, true)
                    && !CONSUMER_PROTOCOL.equals(url.getProtocol());
            boolean skipFailback = t instanceof SkipFailbackWrapperException;
            //如果不需要重试，则抛出异常，否则加入重新注册集合
            if (check || skipFailback) {
                if (skipFailback) {
                    t = t.getCause();
                }
                throw new IllegalStateException(
                    "Failed to register " + url + " to registry " + getUrl().getAddress() + ", cause: " + t
                        .getMessage(), t);
            } else {
                logger.error("Failed to register " + url + ", waiting for retry, cause: " + t.getMessage(), t);
            }

            // Record a failed registration request to a failed list, retry regularly
            //加入重新注册集合中
            addFailedRegistered(url);
        }
    }

    /**
     * 取消注册
     * @param url Registration information , is not allowed to be empty, e.g: dubbo://10.20.153.10/org.apache.dubbo.foo.BarService?version=1.0.0&application=kylin
     */
    @Override
    public void unregister(URL url) {

        //调用父类的取消注册方法
        super.unregister(url);
        //将该URL从注册失败的集合中移除，并将正在进行重试的任务取消
        removeFailedRegistered(url);
        //将该URL从取消注册失败的集合中移除，并将正在进行重试的任务取消
        removeFailedUnregistered(url);
        try {
            // Sending a cancellation request to the server side
            //取消注册，模板方法，需要子类实现
            doUnregister(url);
        } catch (Exception e) {
            Throwable t = e;

            // If the startup detection is opened, the Exception is thrown directly.
            boolean check =
                getUrl().getParameter(Constants.CHECK_KEY, true) && url.getParameter(Constants.CHECK_KEY, true)
                    && !CONSUMER_PROTOCOL.equals(url.getProtocol());
            boolean skipFailback = t instanceof SkipFailbackWrapperException;
            if (check || skipFailback) {
                if (skipFailback) {
                    t = t.getCause();
                }
                throw new IllegalStateException(
                    "Failed to unregister " + url + " to registry " + getUrl().getAddress() + ", cause: " + t
                        .getMessage(), t);
            } else {
                logger.error("Failed to unregister " + url + ", waiting for retry, cause: " + t.getMessage(), t);
            }

            // Record a failed registration request to a failed list, retry regularly
            //异常后添加到取消注册失败的集合中
            addFailedUnregistered(url);
        }
    }

    /**
     * 订阅
     * @param url
     * @param listener
     */
    @Override
    public void subscribe(URL url, NotifyListener listener) {

        super.subscribe(url, listener);
        removeFailedSubscribed(url, listener);
        try {
            // Sending a subscription request to the server side
            doSubscribe(url, listener);
        } catch (Exception e) {
            Throwable t = e;

            List<URL> urls = getCacheUrls(url);
            if (CollectionUtils.isNotEmpty(urls)) {
                notify(url, listener, urls);
                logger.error(
                    "Failed to subscribe " + url + ", Using cached list: " + urls + " from cache file: " + getUrl()
                        .getParameter(FILE_KEY,
                            System.getProperty("user.home") + "/dubbo-registry-" + url.getHost() + ".cache")
                        + ", cause: " + t.getMessage(), t);
            } else {
                // If the startup detection is opened, the Exception is thrown directly.
                boolean check =
                    getUrl().getParameter(Constants.CHECK_KEY, true) && url.getParameter(Constants.CHECK_KEY, true);
                boolean skipFailback = t instanceof SkipFailbackWrapperException;
                if (check || skipFailback) {
                    if (skipFailback) {
                        t = t.getCause();
                    }
                    throw new IllegalStateException("Failed to subscribe " + url + ", cause: " + t.getMessage(), t);
                } else {
                    logger.error("Failed to subscribe " + url + ", waiting for retry, cause: " + t.getMessage(), t);
                }
            }

            // Record a failed registration request to a failed list, retry regularly
            addFailedSubscribed(url, listener);
        }
    }

    /**
     * 取消订阅
     * @param url
     * @param listener
     */
    @Override
    public void unsubscribe(URL url, NotifyListener listener) {
        //调用父类的取消订阅
        super.unsubscribe(url, listener);

        //
        removeFailedSubscribed(url, listener);
        try {
            // Sending a canceling subscription request to the server side
            doUnsubscribe(url, listener);
        } catch (Exception e) {
            Throwable t = e;

            // If the startup detection is opened, the Exception is thrown directly.
            boolean check =
                getUrl().getParameter(Constants.CHECK_KEY, true) && url.getParameter(Constants.CHECK_KEY, true);
            boolean skipFailback = t instanceof SkipFailbackWrapperException;
            if (check || skipFailback) {
                if (skipFailback) {
                    t = t.getCause();
                }
                throw new IllegalStateException(
                    "Failed to unsubscribe " + url + " to registry " + getUrl().getAddress() + ", cause: " + t
                        .getMessage(), t);
            } else {
                logger.error("Failed to unsubscribe " + url + ", waiting for retry, cause: " + t.getMessage(), t);
            }

            // Record a failed registration request to a failed list, retry regularly
            addFailedUnsubscribed(url, listener);
        }
    }

    /**
     * 通知
     * @param url      consumer side url 消费方的URL
     * @param listener listener
     * @param urls     provider latest urls 服务提供方最新的URL
     */
    @Override
    protected void notify(URL url, NotifyListener listener, List<URL> urls) {

        if (url == null) {
            throw new IllegalArgumentException("notify url == null");
        }
        if (listener == null) {
            throw new IllegalArgumentException("notify listener == null");
        }
        try {
            doNotify(url, listener, urls);
        } catch (Exception t) {
            // Record a failed registration request to a failed list, retry regularly
            //异常后加入重新通知的集合中
            addFailedNotified(url, listener, urls);
            logger.error("Failed to notify for subscribe " + url + ", waiting for retry, cause: " + t.getMessage(), t);
        }
    }

    /**
     * 通知
     * @param url
     * @param listener
     * @param urls
     */
    protected void doNotify(URL url, NotifyListener listener, List<URL> urls) {

        super.notify(url, listener, urls);
    }

    /**
     * 恢复
     * @throws Exception
     */
    @Override
    protected void recover() throws Exception {
        // register
        Set<URL> recoverRegistered = new HashSet<URL>(getRegistered());
        if (!recoverRegistered.isEmpty()) {
            if (logger.isInfoEnabled()) {
                logger.info("Recover register url " + recoverRegistered);
            }
            for (URL url : recoverRegistered) {
                addFailedRegistered(url);
            }
        }
        // subscribe
        Map<URL, Set<NotifyListener>> recoverSubscribed = new HashMap<URL, Set<NotifyListener>>(getSubscribed());
        if (!recoverSubscribed.isEmpty()) {
            if (logger.isInfoEnabled()) {
                logger.info("Recover subscribe url " + recoverSubscribed.keySet());
            }
            for (Map.Entry<URL, Set<NotifyListener>> entry : recoverSubscribed.entrySet()) {
                URL url = entry.getKey();
                for (NotifyListener listener : entry.getValue()) {
                    addFailedSubscribed(url, listener);
                }
            }
        }
    }

    /**
     * 注销
     */
    @Override
    public void destroy() {

        super.destroy();
        retryTimer.stop();
    }

    // ==== Template method ====

    /**
     * 注册，该方法是一个模板方法，需要具体的注册中心实现
     *
     * @param url
     */
    public abstract void doRegister(URL url);

    public abstract void doUnregister(URL url);

    public abstract void doSubscribe(URL url, NotifyListener listener);

    public abstract void doUnsubscribe(URL url, NotifyListener listener);

    static class Holder {

        private final URL url;

        private final NotifyListener notifyListener;

        Holder(URL url, NotifyListener notifyListener) {

            if (url == null || notifyListener == null) {
                throw new IllegalArgumentException();
            }
            this.url = url;
            this.notifyListener = notifyListener;
        }

        @Override
        public int hashCode() {

            return url.hashCode() + notifyListener.hashCode();
        }

        @Override
        public boolean equals(Object obj) {

            if (obj instanceof Holder) {
                Holder h = (Holder) obj;
                return this.url.equals(h.url) && this.notifyListener.equals(h.notifyListener);
            } else {
                return false;
            }
        }
    }
}
