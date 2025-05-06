/*
 * Copyright (2021) The Delta Lake Project Authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.fs;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.web.KnoxSWebHdfsFileSystem;

/**
 * AbstractFileSystem implementation for HDFS over the web (secure).
 */
@InterfaceAudience.Public
@InterfaceStability.Evolving
public class KnoxSWebHdfs extends DelegateToFileSystem {
  public static final String SCHEME = KnoxSWebHdfsFileSystem.SCHEME;

  public KnoxSWebHdfs(final URI theUri, final Configuration conf) throws IOException, URISyntaxException {
    super(
      theUri,
      createSWebHdfsFileSystem(conf),
      conf,
      SCHEME,
      false
      );
  }

  private static KnoxSWebHdfsFileSystem createSWebHdfsFileSystem(final Configuration conf) {
      final KnoxSWebHdfsFileSystem fs = new KnoxSWebHdfsFileSystem();
      fs.setConf(conf);
      return fs;
  }
}
