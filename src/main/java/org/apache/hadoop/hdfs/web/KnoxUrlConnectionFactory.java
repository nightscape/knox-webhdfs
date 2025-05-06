/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hdfs.web;

import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLConnection;
import java.security.GeneralSecurityException;
import java.util.Arrays;
import java.util.Optional;
import java.util.stream.Collectors;
import javax.net.ssl.HttpsURLConnection;

import org.apache.commons.codec.binary.Base64;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.web.SSLConnectionConfigurator;
import org.apache.hadoop.hdfs.web.URLConnectionFactory;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.authentication.client.AuthenticationException;
import org.apache.http.conn.ssl.AllowAllHostnameVerifier;

public class KnoxUrlConnectionFactory extends URLConnectionFactory {

  private final Configuration config;
  private final URLConnectionFactory delegate;

  public KnoxUrlConnectionFactory(Configuration config, URLConnectionFactory delegate) throws IOException, GeneralSecurityException {
    super(new SSLConnectionConfigurator(60000, 60000, config));
    this.config = config;
    this.delegate = delegate;
  }

  @Override
  public URLConnection openConnection(URL url) throws IOException {
    URL proxyUrl = rewriteUrl(url);
    URLConnection connection = delegate.openConnection(proxyUrl);
    configureConnection(connection);
    return connection;
  }

  @Override
  public URLConnection openConnection(URL url, boolean isSpnego) throws IOException, AuthenticationException {
    URL proxyUrl = rewriteUrl(url);
    URLConnection connection = delegate.openConnection(proxyUrl, isSpnego);
    configureConnection(connection);
    return connection;
  }

  protected URL rewriteUrl(URL url) throws MalformedURLException {
    String newQuery = Arrays.stream(url.getQuery().split("&"))
      .filter(s -> !s.equals("offset=0"))
      .collect(Collectors.joining("&"));
    String contextPath = readConfig("knox.webhdfs.context").orElse("");
    String newPath = url.getPath().contains(contextPath) ? url.getPath() : contextPath + url.getPath();
    return new URL(url.getProtocol(), url.getHost(), url.getPort(), newPath + "?" + newQuery);
  }

  protected void configureConnection(URLConnection connection) throws IOException, MalformedURLException {
    configureConnectionHostnameVerifier(connection);
    configureConnectionAuthentication(connection);
  }

  private void configureConnectionHostnameVerifier(URLConnection connection) {
    if (!config.getBoolean("knox.webhdfs.verify.hostname", true)) {
      if (connection instanceof HttpsURLConnection) {
        ((HttpsURLConnection) connection).setHostnameVerifier(new AllowAllHostnameVerifier());
      }
    }
  }

  private void configureConnectionAuthentication(URLConnection connection) throws IOException {
    Optional<String> passwordOpt = readConfig("knox.webhdfs.password");
    if (passwordOpt.isPresent()) {
      String password = passwordOpt.get();
      String username = readConfig("knox.webhdfs.username")
        .orElseGet(() -> {
          try {
            return UserGroupInformation.getCurrentUser().getUserName();
          } catch (IOException e) {
            throw new RuntimeException(e);
          }
        });
      String credentials = username + ":" + password;
      String encodedCredentials = Base64.encodeBase64String(credentials.getBytes("UTF-8")).trim();
      connection.setRequestProperty("Authorization", "Basic " + encodedCredentials);
    }
  }

  private Optional<String> readConfig(String parameterName) {
    return Optional.ofNullable(config.get(parameterName))
      .or(() -> Optional.ofNullable(System.getenv(parameterName.replace('.', '_').toUpperCase())));
  }
}
