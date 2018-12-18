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

import java.util.Map;

/**
 * Created by zhaohui.yu
 * 15/12/7
 */
public interface Filter {

    /**
     * 在listener.onMessage之前执行
     *
     * @param message       处理的消息，建议不要修改消息内容
     * @param filterContext 可以在这里保存一些上下文
     * @return 如果返回true则filter链继续往下执行，只要任一filter返回false，则后续的
     * filter链不会执行，并且listener.onMessage也不会执行
     */
    boolean preOnMessage(Message message, Map<String, Object> filterContext);

    /**
     * 在listener.onMessage之后执行，可以做一些资源清理工作
     *
     * @param message       处理的消息
     * @param e             filter链和listener.onMessage抛出的异常
     * @param filterContext 上下文
     */
    void postOnMessage(Message message, Throwable e, Map<String, Object> filterContext);
}
