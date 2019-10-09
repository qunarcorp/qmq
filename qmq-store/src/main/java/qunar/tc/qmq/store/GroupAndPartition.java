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

package qunar.tc.qmq.store;

/**
 * Created by zhaohui.yu
 * 8/3/18
 */
public class GroupAndPartition {
    
    private static final String GROUP_INDEX_DELIM = "@";

    private final String partitionName;

    private final String group;

    public GroupAndPartition(String partitionName, String group) {
        this.partitionName = partitionName;
        this.group = group;
    }

    public static String groupAndPartition(String subject, String group) {
        return group + GROUP_INDEX_DELIM + subject;
    }

    public static GroupAndPartition parse(String groupAndSubject) {
        String[] arr = groupAndSubject.split(GROUP_INDEX_DELIM);
        return new GroupAndPartition(arr[1], arr[0]);
    }

    public String getPartitionName() {
        return partitionName;
    }

    public String getGroup() {
        return group;
    }
}
