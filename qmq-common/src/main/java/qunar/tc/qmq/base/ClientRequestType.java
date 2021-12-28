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

package qunar.tc.qmq.base;

/**
 * The enum Client request type.
 * @author yunfeng.yang
 * @since 2017 /10/16
 */
public enum  ClientRequestType {
    /**
     *Online client request type.
     */
    ONLINE(1),
    /**
     *Heartbeat client request type.
     */
    HEARTBEAT(2);

    private int code;

    ClientRequestType(int code) {
        this.code = code;
    }

    /**
     * Gets code.
     *
     * @return the code
     */
    public int getCode() {
        return code;
    }

    /**
     * Is online boolean.
     *
     * @param code the code
     * @return the boolean
     */
    public static boolean isOnline(int code){
        return HEARTBEAT.code == code;
    }

    /**
     * Is heart beat boolean.
     *
     * @param code the code
     * @return the boolean
     */
    public static boolean isHeartBeat(int code){
        return HEARTBEAT.code == code;
    }
}

