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

package org.apache.zookeeper.server.persistence;
import java.io.File;
import java.io.IOException;
import java.util.Map;

import org.apache.zookeeper.server.DataTree;

/**
 * snapshot interface for the persistence layer.
 * implement this interface for implementing 
 * snapshots.
 *
 * SnapShot只定义了“四个方法，反序列化、序列化、查找最新的snapshot文件、释放资源。
 * 所有的数据，在内存中有一份，都使用DataNnode 来进行树形结构的组织，最后形成为一个
 * DataTree的对象
 *
 * DataTree 每隔一段时间就拍摄快照，将内存中的数据持久化到磁盘上。
 *
 * 理论上来说，数据在磁盘有一份。(快照+未拍摄快照的已提交事务的日志组成)
 * 理论上来说，内存拥有完整的一份: DataTree
 */
public interface SnapShot {
    
    /**
     * deserialize a data tree from the last valid snapshot and 
     * return the last zxid that was deserialized
     *
     * 反序列化数据
     *
     * @param dt the datatree to be deserialized into
     * @param sessions the sessions to be deserialized into
     * @return the last zxid that was deserialized from the snapshot
     * @throws IOException
     */
    long deserialize(DataTree dt, Map<Long, Integer> sessions) 
        throws IOException;
    
    /**
     * persist the datatree and the sessions into a persistence storage
     *
     * 序列化数据
     *
     * @param dt the datatree to be serialized
     * @param sessions 
     * @throws IOException
     */
    void serialize(DataTree dt, Map<Long, Integer> sessions, 
            File name) 
        throws IOException;
    
    /**
     * find the most recent snapshot file
     *
     * 寻找最近的一个快照
     *
     * @return the most recent snapshot file
     * @throws IOException
     */
    File findMostRecentSnapshot() throws IOException;
    
    /**
     * free resources from this snapshot immediately
     * @throws IOException
     */
    void close() throws IOException;
} 
