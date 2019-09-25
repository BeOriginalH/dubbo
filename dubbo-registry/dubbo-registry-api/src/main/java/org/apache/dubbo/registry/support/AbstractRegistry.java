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
import org.apache.dubbo.common.logger.Logger;
import org.apache.dubbo.common.logger.LoggerFactory;
import org.apache.dubbo.common.utils.CollectionUtils;
import org.apache.dubbo.common.utils.ConcurrentHashSet;
import org.apache.dubbo.common.utils.ConfigUtils;
import org.apache.dubbo.common.utils.NamedThreadFactory;
import org.apache.dubbo.common.utils.UrlUtils;
import org.apache.dubbo.registry.NotifyListener;
import org.apache.dubbo.registry.Registry;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.RandomAccessFile;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import static org.apache.dubbo.common.constants.CommonConstants.ANY_VALUE;
import static org.apache.dubbo.common.constants.CommonConstants.APPLICATION_KEY;
import static org.apache.dubbo.common.constants.CommonConstants.FILE_KEY;
import static org.apache.dubbo.common.constants.RegistryConstants.CATEGORY_KEY;
import static org.apache.dubbo.common.constants.RegistryConstants.DEFAULT_CATEGORY;
import static org.apache.dubbo.common.constants.RegistryConstants.DYNAMIC_KEY;
import static org.apache.dubbo.common.constants.RegistryConstants.EMPTY_PROTOCOL;
import static org.apache.dubbo.registry.Constants.REGISTRY_FILESAVE_SYNC_KEY;

/**
 * 抽象注册中心
 * 主要操作都是基于内存操作，将缓存保存到本地，或者将本地的读取到缓存中
 * AbstractRegistry. (SPI, Prototype, ThreadSafe)
 */
public abstract class AbstractRegistry implements Registry {

    // URL address separator, used in file cache, service provider URL separation

    /**
     * 在文件缓存的时候用于分割服务提供方的URL的分隔符
     */
    private static final char URL_SEPARATOR = ' ';

    // URL address separated regular expression for parsing the service provider URL list in the file cache

    /**
     * 在解析服务提供者URL列表的时候提供的正则表达式
     */
    private static final String URL_SPLIT = "\\s+";

    // Max times to retry to save properties to local cache file

    /**
     * 将properties缓存到本地文件的重试次数
     */
    private static final int MAX_RETRY_TIMES_SAVE_PROPERTIES = 3;

    // Log output
    protected final Logger logger = LoggerFactory.getLogger(getClass());

    // Local disk cache, where the special key value.registries records the list of registry centers, and the others are the list of notified service providers

    /**
     * 本地磁盘缓存，其中缓存了一个key值为registries，value是注册中心列表，其他的是缓存服务提供方的列表
     */
    private final Properties properties = new Properties();

    // File cache timing writing

    /**
     * 缓存写入的执行器
     */
    private final ExecutorService registryCacheExecutor = Executors
        .newFixedThreadPool(1, new NamedThreadFactory("DubboSaveRegistryCache", true));

    // Is it synchronized to save the file

    /**
     * 是否同步保存文件标识
     */
    private final boolean syncSaveFile;

    /**
     * 记录最新的缓存版本
     */
    private final AtomicLong lastCacheChanged = new AtomicLong();

    /**
     * 记录重试次数
     */
    private final AtomicInteger savePropertiesRetryTimes = new AtomicInteger();

    /**
     * 已注册的URL集合
     */
    private final Set<URL> registered = new ConcurrentHashSet<>();

    /**
     * 订阅绑定的监视器集合
     */
    private final ConcurrentMap<URL, Set<NotifyListener>> subscribed = new ConcurrentHashMap<>();

    /**
     * 通知集合
     */
    private final ConcurrentMap<URL, Map<String, List<URL>>> notified = new ConcurrentHashMap<>();

    /**
     * 注册中心地址
     */
    private URL registryUrl;

    // Local disk cache file

    /**
     * 本地缓存文件
     */
    private File file;

    /**
     * 构造器
     * 主要工作如下：
     * 1、设置URL地址
     * 2、设置syncSaveFile的值
     * 3、获取本地缓存的文件路径，并加载本地缓存文件到内存properties中
     * 4、通知监听器，URL变化的结果
     *
     * @param url 注册中心地址
     */
    public AbstractRegistry(URL url) {

        //设置URL注册中心的值
        setUrl(url);

        // Start file save timer
        //从URL中获取参数save.file，根据参数的值判断是否同步保存文件
        syncSaveFile = url.getParameter(REGISTRY_FILESAVE_SYNC_KEY, false);

        //获取用户自定义的本地缓存路径，如果不存在，则使用默认缓存路径/{user.home}/.dubbo/dubbo-register-{applicationName}-{url.addr}.cache
        String filename = url.getParameter(FILE_KEY,
            System.getProperty("user.home") + "/.dubbo/dubbo-registry-" + url.getParameter(APPLICATION_KEY) + "-" + url
                .getAddress() + ".cache");

        //创建本地缓存文件
        File file = null;
        if (ConfigUtils.isNotEmpty(filename)) {
            file = new File(filename);
            if (!file.exists() && file.getParentFile() != null && !file.getParentFile().exists()) {
                if (!file.getParentFile().mkdirs()) {
                    throw new IllegalArgumentException(
                        "Invalid registry cache file " + file + ", cause: Failed to create directory " + file
                            .getParentFile() + "!");
                }
            }
        }
        this.file = file;
        // When starting the subscription center,
        // we need to read the local cache file for future Registry fault tolerance processing.
        // 加载本地文件信息到properties中
        loadProperties();

        //当注册之后，通知订阅的消费者
        notify(url.getBackupUrls());
    }

    /**
     * 过滤出空值
     *
     * @param url
     * @param urls
     * @return
     */
    protected static List<URL> filterEmpty(URL url, List<URL> urls) {

        if (CollectionUtils.isEmpty(urls)) {
            List<URL> result = new ArrayList<>(1);
            result.add(url.setProtocol(EMPTY_PROTOCOL));
            return result;
        }
        return urls;
    }

    @Override
    public URL getUrl() {

        return registryUrl;
    }

    protected void setUrl(URL url) {

        if (url == null) {
            throw new IllegalArgumentException("registry url == null");
        }
        this.registryUrl = url;
    }

    public Set<URL> getRegistered() {

        return Collections.unmodifiableSet(registered);
    }

    public Map<URL, Set<NotifyListener>> getSubscribed() {

        return Collections.unmodifiableMap(subscribed);
    }

    public Map<URL, Map<String, List<URL>>> getNotified() {

        return Collections.unmodifiableMap(notified);
    }

    public File getCacheFile() {

        return file;
    }

    public Properties getCacheProperties() {

        return properties;
    }

    public AtomicLong getLastCacheChanged() {

        return lastCacheChanged;
    }

    /**
     * 将properties中的数据保存到缓存文件中
     *
     * @param version
     */
    public void doSaveProperties(long version) {

        //如果传入的版本号小于最新的版本号，直接返回
        if (version < lastCacheChanged.get()) {
            return;
        }
        if (file == null) {
            return;
        }
        // Save
        try {
            //创建一个.lock的文件
            File lockfile = new File(file.getAbsolutePath() + ".lock");
            if (!lockfile.exists()) {
                lockfile.createNewFile();
            }
            try (RandomAccessFile raf = new RandomAccessFile(lockfile, "rw"); FileChannel channel = raf.getChannel()) {
                FileLock lock = channel.tryLock();
                if (lock == null) {
                    throw new IOException("Can not lock the registry cache file " + file.getAbsolutePath()
                        + ", ignore and retry later, maybe multi java process use the file, please config: dubbo.registry.file=xxx.properties");
                }
                // Save
                try {
                    if (!file.exists()) {
                        file.createNewFile();
                    }
                    //保存
                    try (FileOutputStream outputFile = new FileOutputStream(file)) {
                        properties.store(outputFile, "Dubbo Registry Cache");
                    }
                } finally {
                    lock.release();
                }
            }
        } catch (Throwable e) {
            //发生异常，重试次数加一
            savePropertiesRetryTimes.incrementAndGet();
            //重试次数大于最大重试次数，复位重试次数，直接返回
            if (savePropertiesRetryTimes.get() >= MAX_RETRY_TIMES_SAVE_PROPERTIES) {
                logger.warn("Failed to save registry cache file after retrying " + MAX_RETRY_TIMES_SAVE_PROPERTIES
                    + " times, cause: " + e.getMessage(), e);
                savePropertiesRetryTimes.set(0);
                return;
            }
            //版本小于最新的版本，直接返回
            if (version < lastCacheChanged.get()) {
                savePropertiesRetryTimes.set(0);
                return;
            } else {
                //异步重试
                registryCacheExecutor.execute(new SaveProperties(lastCacheChanged.incrementAndGet()));
            }
            logger.warn("Failed to save registry cache file, will retry, cause: " + e.getMessage(), e);
        }
    }

    /**
     * 从本地缓存的file中加载到properties
     */
    private void loadProperties() {

        if (file != null && file.exists()) {
            InputStream in = null;
            try {
                in = new FileInputStream(file);
                properties.load(in);
                if (logger.isInfoEnabled()) {
                    logger.info("Load registry cache file " + file + ", data: " + properties);
                }
            } catch (Throwable e) {
                logger.warn("Failed to load registry cache file " + file, e);
            } finally {
                if (in != null) {
                    try {
                        in.close();
                    } catch (IOException e) {
                        logger.warn(e.getMessage(), e);
                    }
                }
            }
        }
    }

    /**
     * 获取缓存的URL
     * @param url
     * @return
     */
    public List<URL> getCacheUrls(URL url) {

        for (Map.Entry<Object, Object> entry : properties.entrySet()) {
            String key = (String) entry.getKey();
            String value = (String) entry.getValue();
            if (key != null && key.length() > 0 && key.equals(url.getServiceKey()) && (Character.isLetter(key.charAt(0))
                || key.charAt(0) == '_') && value != null && value.length() > 0) {
                String[] arr = value.trim().split(URL_SPLIT);
                List<URL> urls = new ArrayList<>();
                for (String u : arr) {
                    urls.add(URL.valueOf(u));
                }
                return urls;
            }
        }
        return null;
    }

    /**
     * 查询注册列表
     * @param url Query condition, is not allowed to be empty, e.g. consumer://10.20.153.10/org.apache.dubbo.foo.BarService?version=1.0.0&application=kylin
     * @return
     */
    @Override
    public List<URL> lookup(URL url) {

        List<URL> result = new ArrayList<>();
        Map<String, List<URL>> notifiedUrls = getNotified().get(url);
        if (notifiedUrls != null && notifiedUrls.size() > 0) {
            for (List<URL> urls : notifiedUrls.values()) {
                for (URL u : urls) {
                    if (!EMPTY_PROTOCOL.equals(u.getProtocol())) {
                        result.add(u);
                    }
                }
            }
        } else {
            final AtomicReference<List<URL>> reference = new AtomicReference<>();
            NotifyListener listener = reference::set;
            subscribe(url, listener); // Subscribe logic guarantees the first notify to return
            List<URL> urls = reference.get();
            if (CollectionUtils.isNotEmpty(urls)) {
                for (URL u : urls) {
                    if (!EMPTY_PROTOCOL.equals(u.getProtocol())) {
                        result.add(u);
                    }
                }
            }
        }
        return result;
    }

    /**
     * 注册地址,只是将URL地址添加到registered集合中
     */
    @Override
    public void register(URL url) {

        if (url == null) {
            throw new IllegalArgumentException("register url == null");
        }
        if (logger.isInfoEnabled()) {
            logger.info("Register: " + url);
        }

        registered.add(url);
    }

    /**
     * 取消注册，将URL从registered中移除
     *
     * @param url Registration information , is not allowed to be empty, e.g: dubbo://10.20.153.10/org.apache.dubbo.foo.BarService?version=1.0.0&application=kylin
     */
    @Override
    public void unregister(URL url) {

        if (url == null) {
            throw new IllegalArgumentException("unregister url == null");
        }
        if (logger.isInfoEnabled()) {
            logger.info("Unregister: " + url);
        }
        registered.remove(url);
    }

    /**
     * 订阅
     * @param url      Subscription condition, not allowed to be empty, e.g. consumer://10.20.153.10/org.apache.dubbo.foo.BarService?version=1.0.0&application=kylin
     * @param listener A listener of the change event, not allowed to be empty
     */
    @Override
    public void subscribe(URL url, NotifyListener listener) {

        if (url == null) {
            throw new IllegalArgumentException("subscribe url == null");
        }
        if (listener == null) {
            throw new IllegalArgumentException("subscribe listener == null");
        }
        if (logger.isInfoEnabled()) {
            logger.info("Subscribe: " + url);
        }
        Set<NotifyListener> listeners = subscribed.computeIfAbsent(url, n -> new ConcurrentHashSet<>());
        listeners.add(listener);
    }

    /**
     * 取消订阅
     * @param url      Subscription condition, not allowed to be empty, e.g. consumer://10.20.153.10/org.apache.dubbo.foo.BarService?version=1.0.0&application=kylin
     * @param listener A listener of the change event, not allowed to be empty
     */
    @Override
    public void unsubscribe(URL url, NotifyListener listener) {

        if (url == null) {
            throw new IllegalArgumentException("unsubscribe url == null");
        }
        if (listener == null) {
            throw new IllegalArgumentException("unsubscribe listener == null");
        }
        if (logger.isInfoEnabled()) {
            logger.info("Unsubscribe: " + url);
        }
        Set<NotifyListener> listeners = subscribed.get(url);
        if (listeners != null) {
            listeners.remove(listener);
        }
    }

    /**
     * 恢复
     * @throws Exception
     */
    protected void recover() throws Exception {
        // register
        //获得所有的注册URL
        Set<URL> recoverRegistered = new HashSet<>(getRegistered());
        if (!recoverRegistered.isEmpty()) {
            if (logger.isInfoEnabled()) {
                logger.info("Recover register url " + recoverRegistered);
            }
            //重新注册
            for (URL url : recoverRegistered) {
                register(url);
            }
        }
        // subscribe
        //获得所有的订阅URL
        Map<URL, Set<NotifyListener>> recoverSubscribed = new HashMap<>(getSubscribed());
        if (!recoverSubscribed.isEmpty()) {
            if (logger.isInfoEnabled()) {
                logger.info("Recover subscribe url " + recoverSubscribed.keySet());
            }
            for (Map.Entry<URL, Set<NotifyListener>> entry : recoverSubscribed.entrySet()) {
                URL url = entry.getKey();
                for (NotifyListener listener : entry.getValue()) {
                    //重新订阅
                    subscribe(url, listener);
                }
            }
        }
    }

    /**
     * 通知消费者的监听器，url改变
     *
     * @param urls
     */
    protected void notify(List<URL> urls) {

        if (CollectionUtils.isEmpty(urls)) {
            return;
        }

        //获取所有订阅的集合
        for (Map.Entry<URL, Set<NotifyListener>> entry : getSubscribed().entrySet()) {
            URL url = entry.getKey();

            //如果订阅的集合中的URL不是当前注册的URL，则忽略
            if (!UrlUtils.isMatch(url, urls.get(0))) {
                continue;
            }

            //获取当前URL的所有通知监听器
            Set<NotifyListener> listeners = entry.getValue();
            if (listeners != null) {
                for (NotifyListener listener : listeners) {
                    try {
                        notify(url, listener, filterEmpty(url, urls));
                    } catch (Throwable t) {
                        logger
                            .error("Failed to notify registry event, urls: " + urls + ", cause: " + t.getMessage(), t);
                    }
                }
            }
        }
    }

    /**
     * 服务提供方给订阅费发送数据改变通知
     * Notify changes from the Provider side.
     *
     * @param url      consumer side url 消费方的URL
     * @param listener listener
     * @param urls     provider latest urls 服务提供方最新的URL
     */
    protected void notify(URL url, NotifyListener listener, List<URL> urls) {

        if (url == null) {
            throw new IllegalArgumentException("notify url == null");
        }
        if (listener == null) {
            throw new IllegalArgumentException("notify listener == null");
        }
        if ((CollectionUtils.isEmpty(urls)) && !ANY_VALUE.equals(url.getServiceInterface())) {
            logger.warn("Ignore empty notify urls for subscribe url " + url);
            return;
        }
        if (logger.isInfoEnabled()) {
            logger.info("Notify urls for subscribe url " + url + ", urls: " + urls);
        }
        // keep every provider's category.
        //根据category分类
        Map<String, List<URL>> result = new HashMap<>();
        for (URL u : urls) {
            if (UrlUtils.isMatch(url, u)) {
                String category = u.getParameter(CATEGORY_KEY, DEFAULT_CATEGORY);
                List<URL> categoryList = result.computeIfAbsent(category, k -> new ArrayList<>());
                categoryList.add(u);
            }
        }
        if (result.size() == 0) {
            return;
        }
        Map<String, List<URL>> categoryNotified = notified.computeIfAbsent(url, u -> new ConcurrentHashMap<>());
        for (Map.Entry<String, List<URL>> entry : result.entrySet()) {
            String category = entry.getKey();
            List<URL> categoryList = entry.getValue();
            categoryNotified.put(category, categoryList);
            listener.notify(categoryList);
            // We will update our cache file after each notification.
            // When our Registry has a subscribe failure due to network jitter, we can return at least the existing cache URL.
            saveProperties(url);
        }
    }

    /** 将properties文件保存到本地
     * @param url
     */
    private void saveProperties(URL url) {

        if (file == null) {
            return;
        }

        try {
            StringBuilder buf = new StringBuilder();
            //根据URL获取单个URL下所有的通知地址，并按空格进行拼接
            Map<String, List<URL>> categoryNotified = notified.get(url);
            if (categoryNotified != null) {
                for (List<URL> us : categoryNotified.values()) {
                    for (URL u : us) {
                        if (buf.length() > 0) {
                            buf.append(URL_SEPARATOR);
                        }
                        buf.append(u.toFullString());
                    }
                }
            }
            //将拼接后的URL根据接口名作为key保存
            properties.setProperty(url.getServiceKey(), buf.toString());
            //版本号递增
            long version = lastCacheChanged.incrementAndGet();
            //如果是同步保存
            if (syncSaveFile) {
                doSaveProperties(version);
            } else {
                //调用执行器执行
                registryCacheExecutor.execute(new SaveProperties(version));
            }
        } catch (Throwable t) {
            logger.warn(t.getMessage(), t);
        }
    }

    /**
     * 重写Node的destroy方法
     */
    @Override
    public void destroy() {

        if (logger.isInfoEnabled()) {
            logger.info("Destroy registry:" + getUrl());
        }
        //获取所有注册的URL
        Set<URL> destroyRegistered = new HashSet<>(getRegistered());
        if (!destroyRegistered.isEmpty()) {
            for (URL url : new HashSet<>(getRegistered())) {
                //如果URL中设置了dynamic=true,才将URL从集合中移除
                if (url.getParameter(DYNAMIC_KEY, true)) {
                    try {
                        //取消注册
                        unregister(url);
                        if (logger.isInfoEnabled()) {
                            logger.info("Destroy unregister url " + url);
                        }
                    } catch (Throwable t) {
                        logger.warn(
                            "Failed to unregister url " + url + " to registry " + getUrl() + " on destroy, cause: " + t
                                .getMessage(), t);
                    }
                }
            }
        }
        //获得所有订阅的URL
        Map<URL, Set<NotifyListener>> destroySubscribed = new HashMap<>(getSubscribed());
        if (!destroySubscribed.isEmpty()) {
            for (Map.Entry<URL, Set<NotifyListener>> entry : destroySubscribed.entrySet()) {
                URL url = entry.getKey();
                for (NotifyListener listener : entry.getValue()) {
                    try {
                        //取消订阅
                        unsubscribe(url, listener);
                        if (logger.isInfoEnabled()) {
                            logger.info("Destroy unsubscribe url " + url);
                        }
                    } catch (Throwable t) {
                        logger.warn(
                            "Failed to unsubscribe url " + url + " to registry " + getUrl() + " on destroy, cause: " + t
                                .getMessage(), t);
                    }
                }
            }
        }
    }

    @Override
    public String toString() {

        return getUrl().toString();
    }

    /**
     * 保存prope文件到本地
     */
    private class SaveProperties implements Runnable {

        private long version;

        private SaveProperties(long version) {

            this.version = version;
        }

        @Override
        public void run() {

            doSaveProperties(version);
        }
    }

}
