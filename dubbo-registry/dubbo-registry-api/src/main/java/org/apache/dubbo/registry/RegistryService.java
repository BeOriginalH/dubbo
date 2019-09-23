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
package org.apache.dubbo.registry;

import org.apache.dubbo.common.URL;

import java.util.List;

/**
 * 注册中心模块的服务接口，提供注册、取消注册、订阅、取消订阅、查询符合条件的已注册的数据
 * RegistryService. (SPI, Prototype, ThreadSafe)
 *
 * @see org.apache.dubbo.registry.Registry
 * @see org.apache.dubbo.registry.RegistryFactory#getRegistry(URL)
 */
public interface RegistryService {

    /**
     * Register data, such as : provider service, consumer address, route rule, override rule and other data.
     * 注册数据，比如提供者服务、消费者地址、路由规则、覆盖规则等其他信息
     * <p>
     * Registering is required to support the contract:<br>
     * 1. When the URL sets the check=false parameter. When the registration fails, the exception is not thrown and retried in the background. Otherwise, the exception will be thrown.<br>
     * 2. When URL sets the dynamic=false parameter, it needs to be stored persistently, otherwise, it should be deleted automatically when the registrant has an abnormal exit.<br>
     * 3. When the URL sets category=routers, it means classified storage, the default category is providers, and the data can be notified by the classified section. <br>
     * 4. When the registry is restarted, network jitter, data can not be lost, including automatically deleting data from the broken line.<br>
     * 5. Allow URLs which have the same URL but different parameters to coexist,they can't cover each other.<br>
     *
     * 注册需要满足以下的约定：
     * 1、如果URL设置了check=false参数，当注册失败的时候，应用不会抛出异常，而是在后台进行重试，否则异常会抛出
     * 2、如果URL设置了dynamic=false参数，会被立刻持久化，否则注册异常的时候会被删除
     * 3、当URL设置了category=routers，标识存储分类，默认类别是提供者，并且可以通过分类通知
     * 4、当注册中心重启的时候，或者网络抖动，数据不会丢失
     * 5、所有的URL如果URL路径相同，但是参数不一样，可以同时存在，不能被覆盖
     *
     * @param url  Registration information , is not allowed to be empty, e.g: dubbo://10.20.153.10/org.apache.dubbo.foo.BarService?version=1.0.0&application=kylin
     */
    void register(URL url);

    /**
     * Unregister
     * 取消注册
     * <p>
     * Unregistering is required to support the contract:<br>
     * 1. If it is the persistent stored data of dynamic=false, the registration data can not be found, then the IllegalStateException is thrown, otherwise it is ignored.<br>
     * 2. Unregister according to the full url match.<br>
     *
     *取消注册需要满足以下约定：
     * 1、如果是设置了dynamic=falsed的持久化数据，当注册的数据找不到的时候会抛出异常，否则忽略
     * 2、取消注册需要根据URL完全路径匹配（包括参数）
     *
     * @param url Registration information , is not allowed to be empty, e.g: dubbo://10.20.153.10/org.apache.dubbo.foo.BarService?version=1.0.0&application=kylin
     */
    void unregister(URL url);

    /**
     * 订阅合法的注册数据，并且在注册的数据改变的时候立刻进行通知
     * Subscribe to eligible registered data and automatically push when the registered data is changed.
     * <p>
     * Subscribing need to support contracts:<br>
     * 1. When the URL sets the check=false parameter. When the registration fails, the exception is not thrown and retried in the background. <br>
     * 2. When URL sets category=routers, it only notifies the specified classification data. Multiple classifications are separated by commas,
     * and allows asterisk to match, which indicates that all categorical data are subscribed.<br>
     * 3. Allow interface, group, version, and classifier as a conditional query, e.g.: interface=org.apache.dubbo.foo.BarService&version=1.0.0<br>
     * 4. And the query conditions allow the asterisk to be matched, subscribe to all versions of all the packets of all interfaces, e.g. :interface=*&group=*&version=*&classifier=*<br>
     * 5. When the registry is restarted and network jitter, it is necessary to automatically restore the subscription request.<br>
     * 6. Allow URLs which have the same URL but different parameters to coexist,they can't cover each other.<br>
     * 7. The subscription process must be blocked, when the first notice is finished and then returned.<br>
     *
     * 订阅需要满足以下的约定：
     * 1、如果URL参数设置了check=false，当注册失败时（怎么是注册？应该是订阅的程序不会抛出服务找不到的异常吧？），异常不会抛出并且在后台重试
     * 2、当URL参数中设置了category=routers,只会通知指定类别的数据，多个类别使用逗号分隔，并且可以使用星号表示所有的分类数据都订阅
     * 3、允许使用接口、group、version、分类作为查询条件
     * 4、允许使用星号匹配，订阅所有的版本、所有的包、所有的接口
     * 5、当注册中心重启或者网络抖动，有必要的时候要恢复订阅请求
     * 6、所有的URL如果URL路径相同，但是参数不一样，可以同时存在，不能被覆盖
     * 7、当第一个通知完成和范尼时，订阅过程必须阻塞
     *
     * @param url      Subscription condition, not allowed to be empty, e.g. consumer://10.20.153.10/org.apache.dubbo.foo.BarService?version=1.0.0&application=kylin
     * @param listener A listener of the change event, not allowed to be empty
     */
    void subscribe(URL url, NotifyListener listener);

    /**
     * 取消订阅
     * Unsubscribe
     * <p>
     * Unsubscribing is required to support the contract:<br>
     * 1. If don't subscribe, ignore it directly.<br>
     * 2. Unsubscribe by full URL match.<br>
     *
     *  取消注册需要满足以下条件：
     *  1、如果没有订阅，立刻忽略
     *  2、取消订阅需要使用完整的URL路径
     *
     * @param url      Subscription condition, not allowed to be empty, e.g. consumer://10.20.153.10/org.apache.dubbo.foo.BarService?version=1.0.0&application=kylin
     * @param listener A listener of the change event, not allowed to be empty
     */
    void unsubscribe(URL url, NotifyListener listener);

    /**
     * Query the registered data that matches the conditions. Corresponding to the push mode of the subscription, this is the pull mode and returns only one result.
     * 根据条件查询注册的数据，
     *
     * @param url Query condition, is not allowed to be empty, e.g. consumer://10.20.153.10/org.apache.dubbo.foo.BarService?version=1.0.0&application=kylin
     * @return The registered information list, which may be empty, the meaning is the same as the parameters of {@link org.apache.dubbo.registry.NotifyListener#notify(List<URL>)}.
     * @see org.apache.dubbo.registry.NotifyListener#notify(List)
     */
    List<URL> lookup(URL url);

}
