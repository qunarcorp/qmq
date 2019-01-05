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

package qunar.tc.qmq.tools;

import picocli.CommandLine;
import picocli.CommandLine.Command;
import qunar.tc.qmq.tools.command.*;

/**
 * @author keli.wang
 * @since 2018-12-05
 */
@Command(name = "tools.sh", mixinStandardHelpOptions = true)
public class Tools implements Runnable {
    public static void main(String[] args) {
        final MetaManagementService service = new MetaManagementService();

        final CommandLine cmd = new CommandLine(new Tools());
        cmd.addSubcommand("AddBroker", new AddBrokerCommand(service));
        cmd.addSubcommand("AddDb", new AddDbCommand(service));
        cmd.addSubcommand("ReplaceBroker", new ReplaceBrokerCommand(service));
        cmd.addSubcommand("ListBrokers", new ListBrokersCommand(service));
        cmd.addSubcommand("ListBrokerGroups", new ListBrokerGroupsCommand(service));
        cmd.addSubcommand("ListSubjectRoutes", new ListSubjectRoutesCommand(service));
        cmd.addSubcommand("AddSubjectBrokerGroup", new AddSubjectBrokerGroupCommand(service));
        cmd.addSubcommand("RemoveSubjectBrokerGroup", new RemoveSubjectBrokerGroupCommand(service));
        cmd.addSubcommand("AddNewSubject", new AddNewSubjectCommand(service));
        cmd.addSubcommand("ExtendSubjectRoute", new ExtendSubjectRouteCommand(service));
        cmd.addSubcommand("MarkReadonly", new MarkBrokerReadonlyCommand(service));
        cmd.addSubcommand("UnMarkReadonly", new UnMarkBrokerReadonlyCommand(service));

        cmd.parseWithHandler(new CommandLine.RunLast(), args);
    }

    @Override
    public void run() {
        CommandLine.usage(this, System.out);
    }
}
