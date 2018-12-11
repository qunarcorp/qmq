/*
 * Copyright 2018 Qunar
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
 * limitations under the License.com.qunar.pay.trade.api.card.service.usercard.UserCardQueryFacade
 */

package qunar.tc.qmq.producer.tx;


/**
 * @author miao.yang susing@gmail.com
 * @date 2013-1-9
 */
public class SqlConstant {
    public static final String insertSQL = "INSERT INTO qmq_produce.qmq_msg_queue(content,create_time) VALUES(?,?)";
    public static final String blockSQL = "UPDATE qmq_produce.qmq_msg_queue SET status=-100,error=error+1,update_time=? WHERE id=?";
    public static final String finishSQL = "DELETE FROM qmq_produce.qmq_msg_queue WHERE id=?";
}
