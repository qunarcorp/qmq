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
 * limitations under the License.
 */

package qunar.tc.qmq.tools;

import com.ning.http.client.AsyncHttpClient;
import com.ning.http.client.Response;

import java.util.Map;

/**
 * @author keli.wang
 * @since 2018-12-05
 */
public class MetaManagementService {
    private final AsyncHttpClient client;

    public MetaManagementService() {
        this.client = new AsyncHttpClient();
    }

    public String post(final String metaServer, final String token, final Map<String, String> params) {
        try {
            final String url = String.format("http://%s/management", metaServer);
            final AsyncHttpClient.BoundRequestBuilder builder = client.preparePost(url);
            builder.addHeader("X-Api-Token", token);
            params.forEach(builder::addQueryParam);
            final Response response = builder.execute().get();
            return response.getResponseBody("UTF-8");
        } catch (Exception e) {
            throw new RuntimeException("send request meta server failed.", e);
        }
    }

    public void close() {
        client.close();
    }
}
