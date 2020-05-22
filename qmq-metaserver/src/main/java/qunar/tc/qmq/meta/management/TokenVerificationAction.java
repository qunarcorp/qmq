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

import com.google.common.collect.ImmutableMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import qunar.tc.qmq.configuration.DynamicConfig;
import qunar.tc.qmq.configuration.DynamicConfigLoader;

import javax.servlet.http.HttpServletRequest;

/**
 * @author keli.wang
 * @since 2017/10/23
 */
public class TokenVerificationAction implements MetaManagementAction {
    private static final Logger LOG = LoggerFactory.getLogger(TokenVerificationAction.class);

    private static final String TOKEN_HEADER = "X-Api-Token";

    private static volatile ImmutableMap<String, String> validApiTokens = ImmutableMap.of();

    static {
        final DynamicConfig config = DynamicConfigLoader.load("valid-api-tokens.properties", false);
        config.addListener(conf -> validApiTokens = ImmutableMap.copyOf(conf.asMap()));
    }

    private final MetaManagementAction action;

    public TokenVerificationAction(final MetaManagementAction action) {
        this.action = action;
    }

    @Override
    public Object handleAction(final HttpServletRequest req) {
        final String token = req.getHeader(TOKEN_HEADER);
        if (!validApiTokens.containsKey(token)) {
            return ActionResult.error("没有提供合法的 Api Token");
        }

        LOG.info("{} passed token verification.", validApiTokens.get(token));
        return action.handleAction(req);
    }
}
