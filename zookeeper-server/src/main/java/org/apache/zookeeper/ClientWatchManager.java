/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.zookeeper;

import java.util.Set;

/**
 * ClientWatchManager主要用来给客户端返回 一堆需要处理的watcher
 */
public interface ClientWatchManager {
    /**
     * Return a set of watchers that should be notified of the event. The 
     * manager must not notify the watcher(s), however it will update it's 
     * internal structure as if the watches had triggered. The intent being 
     * that the callee is now responsible for notifying the watchers of the 
     * event, possibly at some later time.
     *
     * 根据zk返回回来的消息来决定到底回调那个watcher
     * 该方法表示事件发生时，返回需要被通知的Watcher集合， 可能为空集合。
     *
     * 这个方法的三个参数组装成个对象。 其实就是 WatedEvent
     * 这就是服务端给你传递回来的三个参数，在客户端就会调用构造方法，把这三个参数构造成watchedEvent
     *
     * 
     * @param state event state
     * @param type event type
     * @param path event path
     * @return may be empty set but must not be null
     */
    public Set<Watcher> materialize(Watcher.Event.KeeperState state,
        Watcher.Event.EventType type, String path);
}
