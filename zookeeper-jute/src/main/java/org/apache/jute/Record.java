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

package org.apache.jute;

import org.apache.yetus.audience.InterfaceAudience;

import java.io.IOException;

/**
 * Interface that is implemented by generated classes.
 *
 * 如果在zookeeper中，某些类需要进行序列化以及反序列化，都需要实现这个接口
 * 
 */
@InterfaceAudience.Public
public interface Record {

    // 真正的序列化接口（把java对象，写入到网络流中，持久化到磁盘中）
    public void serialize(OutputArchive archive, String tag)
        throws IOException;

    // 真正的反序列化方法（从网络流中读取对象）
    public void deserialize(InputArchive archive, String tag)
        throws IOException;
}
