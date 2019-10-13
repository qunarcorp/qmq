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

package qunar.tc.qmq.protocol;

/**
 * @author yiqun.fan create on 17-7-4.
 */
public class RemotingHeader {

    public static final int DEFAULT_MAGIC_CODE = 0xdec1_0ade;


    private static final short VERSION_4 = 4;
    /**
     * add schedule time in message header for delay message
     */
    private static final short VERSION_7 = 7;

    /**
     * add tags field for message header
     */
    private static final short VERSION_8 = 8;

    /**
     * add pull request filters
     */
    private static final short FILTER_VERSION = 9;

    /**
     * ordered message version
     */
    private static final short ORDERED_MESSAGE_VERSION = 10;

    public static short getScheduleTimeVersion() {
        return VERSION_7;
    }

    public static boolean supportTags(int version) {
        return version >= VERSION_8;
    }

    public static boolean supportFilter(int version) {
        return version >= FILTER_VERSION;
    }

    public static boolean supportOrderedMessage(int version) {
        return version >= ORDERED_MESSAGE_VERSION;
    }

    public static short getOrderedMessageVersion() {
        return ORDERED_MESSAGE_VERSION;
    }

    public static short getCurrentVersion() {
        return ORDERED_MESSAGE_VERSION;
    }

    public static final short MIN_HEADER_SIZE = 18;  // magic code(4) + code(2) + version(2) + opaque(4) + flag(4) + request code(2)
    public static final short HEADER_SIZE_LEN = 2;
    public static final short TOTAL_SIZE_LEN = 4;

    public static final short LENGTH_FIELD = TOTAL_SIZE_LEN + HEADER_SIZE_LEN;

    private int magicCode = DEFAULT_MAGIC_CODE;
    private short code;
    private short version = getCurrentVersion();
    private int opaque;
    private int flag;
    private short requestCode = CommandCode.PLACEHOLDER;

    public int getMagicCode() {
        return magicCode;
    }

    public void setMagicCode(int magicCode) {
        this.magicCode = magicCode;
    }

    public short getCode() {
        return code;
    }

    public void setCode(short code) {
        this.code = code;
    }

    public short getVersion() {
        return version;
    }

    public void setVersion(short version) {
        this.version = version;
    }

    public int getOpaque() {
        return opaque;
    }

    public void setOpaque(int opaque) {
        this.opaque = opaque;
    }

    public int getFlag() {
        return flag;
    }

    public void setFlag(int flag) {
        this.flag = flag;
    }

    public short getRequestCode() {
        return requestCode;
    }

    public void setRequestCode(short requestCode) {
        this.requestCode = requestCode;
    }


    @Override
    public String toString() {
        return "RemotingHeader{" +
                "magicCode=" + magicCode +
                ", code=" + code +
                ", version=" + version +
                ", opaque=" + opaque +
                ", flag=" + flag +
                ", requestCode=" + requestCode +
                '}';
    }
}
