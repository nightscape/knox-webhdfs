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
package org.apache.hadoop.gateway.hdfs.web;

import org.apache.commons.codec.binary.Base64;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.web.URLConnectionFactory;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.alias.CredentialShell;
import org.apache.hadoop.security.authentication.client.AuthenticationException;
import org.apache.http.conn.ssl.AllowAllHostnameVerifier;

import javax.net.ssl.HttpsURLConnection;
import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLConnection;

public class KnoxUrlConnectionFactory extends URLConnectionFactory {

  private Configuration config;
  private URLConnectionFactory delegate;

  public KnoxUrlConnectionFactory( Configuration config, URLConnectionFactory delegate ) {
    super( config );
    this.config = config;
    this.delegate = delegate;
  }

  @Override
  public URLConnection openConnection(URL url) throws IOException {
    URL proxy = rewriteUrl( url );
    URLConnection connection = delegate.openConnection( proxy );
    configureConnection( connection );
    return connection;
  }

  @Override
  public URLConnection openConnection(URL url, boolean isSpnego) throws IOException, AuthenticationException {
    URL proxyUrl = rewriteUrl( url );
    URLConnection connection = delegate.openConnection( proxyUrl, isSpnego );
    configureConnection( connection );
    return connection;
  }

  protected URL rewriteUrl( URL url ) throws MalformedURLException {
    url = new URL( url.getProtocol(), url.getHost(), url.getPort(), config.get("knox.webhdfs.context") + url.getFile() );
    return url;
  }

  protected void configureConnection( URLConnection connection ) throws IOException {
    configureConnectionHostnameVerifier( connection );
    configureConnectionAuthentication( connection );
  }

  private void configureConnectionHostnameVerifier( URLConnection connection ) {
    if( !config.getBoolean( "knox.webhdfs.verify.hostname", true ) ) {
      if( connection instanceof HttpsURLConnection ) {
        HttpsURLConnection httpsConnection = (HttpsURLConnection)connection;
        httpsConnection.setHostnameVerifier( new AllowAllHostnameVerifier() );
      }
    }
  }

  private void configureConnectionAuthentication( URLConnection connection ) throws IOException {
    String password = config.get( "knox.webhdfs.password" );
    if( password != null ) {
      String username = UserGroupInformation.getCurrentUser().getUserName();
      if( password.equals( "" ) ) {
        CredentialShell.PasswordReader reader = new CredentialShell.PasswordReader();
        password = new String( reader.readPassword( "Password for " + username + ":" ) );
      }
      String credentials = username + ":" + password;
      String encodedCredentials = Base64.encodeBase64String( credentials.getBytes() ).trim();
      connection.setRequestProperty( "Authorization", "Basic " + encodedCredentials );
    }
  }

}
