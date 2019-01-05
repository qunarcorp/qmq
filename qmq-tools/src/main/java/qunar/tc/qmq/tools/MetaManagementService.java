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

import com.google.common.io.CharStreams;

import java.io.*;
import java.net.HttpURLConnection;
import java.net.URL;
import java.net.URLEncoder;
import java.util.Map;


/**
 * @author keli.wang
 * @since 2018-12-05
 */
public class MetaManagementService {

    public String post(final String metaServer, final String token, final Map<String, String> params) {
        BufferedWriter writer = null;
        InputStreamReader reader = null;
        try {
            final String url = String.format("http://%s/management", metaServer);
            HttpURLConnection connection = (HttpURLConnection) new URL(url).openConnection();
            connection.setConnectTimeout(2000);
            connection.setReadTimeout(2000);
            connection.setDoInput(true);
            connection.setDoOutput(true);
            connection.setRequestProperty("X-Api-Token", token);
            connection.setRequestMethod("POST");
            OutputStream os = connection.getOutputStream();
            writer = new BufferedWriter(new OutputStreamWriter(os, "UTF-8"));
            writer.write(getPostData(params));
            writer.flush();

            reader = new InputStreamReader(connection.getInputStream());
            String content = CharStreams.toString(reader);
            if (connection.getResponseCode() != 200) {
                throw new RuntimeException("send request failed");
            }
            return content.trim();
        } catch (Exception e) {
            throw new RuntimeException("send request meta server failed.", e);
        } finally {
            closeQuietly(writer);
            closeQuietly(reader);
        }
    }

    private void closeQuietly(Closeable closeable) {
        if (closeable == null) return;
        try {
            closeable.close();
        } catch (Exception ignore) {
        }
    }

    private String getPostData(Map<String, String> params) throws UnsupportedEncodingException {
        boolean first = true;
        StringBuilder result = new StringBuilder();
        for (Map.Entry<String, String> entry : params.entrySet()) {
            if (first) {
                first = false;
            } else {
                result.append("&");
            }
            result.append(URLEncoder.encode(entry.getKey(), "UTF-8"));
            result.append("=");
            result.append(URLEncoder.encode(entry.getValue(), "UTF-8"));
        }
        return result.toString();
    }
}
