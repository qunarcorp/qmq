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

package qunar.tc.qmq.task.store;

import qunar.tc.qmq.task.model.DataSourceInfoModel;
import qunar.tc.qmq.task.model.DataSourceInfoStatus;

import java.util.List;

/**
 * Created by IntelliJ IDEA.
 * User: liuzz
 * Date: 12-12-21
 * Time: 下午7:29
 */
public interface IDataSourceConfigStore {

    List<DataSourceInfoModel> findDataSourceInfos(DataSourceInfoStatus status, String namespace);
}
