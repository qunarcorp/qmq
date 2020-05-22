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

package qunar.tc.qmq.meta.management;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * @author keli.wang
 * @since 2018/7/30
 */
public class ActionResult<T> {
    private final int status;
    private final String message;
    private final T data;

    @JsonCreator
    public ActionResult(@JsonProperty("status") final int status, @JsonProperty("message") final String message, @JsonProperty("data") final T data) {
        this.status = status;
        this.message = message;
        this.data = data;
    }

    public static <T> ActionResult<T> error(final String message) {
        return new ActionResult<>(-1, message, null);
    }

    public static <T> ActionResult<T> error(final String message, final T data) {
        return new ActionResult<>(-1, message, data);
    }

    public static <T> ActionResult<T> ok(final T data) {
        return new ActionResult<>(0, "", data);
    }

    public int getStatus() {
        return status;
    }

    public String getMessage() {
        return message;
    }

    public T getData() {
        return data;
    }
}
