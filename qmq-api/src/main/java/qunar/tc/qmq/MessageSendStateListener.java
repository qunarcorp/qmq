/*
 * Copyright 2018 Qunar, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package qunar.tc.qmq;

/**
 * User: zhaohuiyu
 * Date: 5/24/13
 * Time: 3:31 PM
 * <p/>
 * 消息的发送状态监听器
 */
public interface MessageSendStateListener {
    /**
     * 消息成功发送后触发
     *
     * @param message
     */
    void onSuccess(Message message);

    /**
     * 消息发送失败时触发
     *
     * @param message
     */
    void onFailed(Message message);
}
