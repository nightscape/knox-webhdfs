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
import java.net.URI;
import java.net.URL;
import java.security.GeneralSecurityException;
import java.util.Arrays;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.web.resources.HttpOpParam;
import org.apache.hadoop.hdfs.web.resources.Param;

public class KnoxWebHdfsFileSystem extends WebHdfsFileSystem {
  public static final String SCHEME = "knoxwebhdfs";
  public static final int DEFAULT_PORT = 8443;

  @Override
  public synchronized void initialize(URI uri, Configuration conf) throws IOException {
    super.initialize(uri, conf);
    try {
      this.connectionFactory = new KnoxUrlConnectionFactory(conf, this.connectionFactory);
    } catch (GeneralSecurityException e) {
      throw new IOException(e);
    }
  }

  @Override
  public URL toUrl(HttpOpParam.Op op, Path fspath, Param<?, ?>... parameters) throws IOException {
    Param<?, ?>[] newParams = Arrays.stream(parameters)
      .filter(param -> !(param.getName().equals("offset") && param.getValue().equals(0L)))
      .toArray(Param<?, ?>[]::new);
    return super.toUrl(op, fspath, newParams);
  }

  @Override
  public String getScheme() {
    return SCHEME;
  }

  @Override
  protected String getTransportScheme() {
    return "http";
  }

  @Override
  protected int getDefaultPort() {
    return DEFAULT_PORT;
  }
}
