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
package net.minder.knox.webhdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

public class Test {

  public static void main( String args[] ) throws IOException, InterruptedException, URISyntaxException {
    //directAccess();
    proxyAccess();
  }

  public static void directAccess() throws URISyntaxException, IOException, InterruptedException {

    String NAMENODE_HOST = "172.22.112.9";
    String NAMENODE_PORT = "50070";

    String basePath = "webhdfs://" + NAMENODE_HOST + ":" + NAMENODE_PORT + "/user";

    Configuration conf = new Configuration();
    // conf.set( "fs.defaultFS", basePath );
    FileSystem fs = FileSystem.get( new URI( basePath ), conf, "hdfs" );

    FileStatus[] status = fs.listStatus( new Path( basePath ) );
    for( int i = 0; i < status.length; i++ ) {
      System.out.println( status[ i ].getPath() );
    }
    // BufferedReader br=new BufferedReader(new InputStreamReader
    //     (fs.open(new Path("webhdfs://ivlhdp95:50070/user/chandu/twitter.avro"))));
    // String line;
    // line=br.readLine();
    // while (line != null){
    //   System.out.println(line);
    //   line=br.readLine();
    // }
    //
    // BufferedWriter pw = new BufferedWriter(
    //     new OutputStreamWriter(fs.create(new Path("webhdfs://ivlhdp95:50070/user/chandu/abc.txt"),true)));
    // pw.write("abcd");
    // pw.close();
  }

  public static void proxyAccess() throws URISyntaxException, IOException, InterruptedException {
    System.setProperty( "javax.net.ssl.trustStore", "/Users/kevin.minder/Projects/knox-webhdfs/gateway.jks" );

    String uri = "knoxswebhdfs://172.22.112.9:8443";

    Configuration conf = new Configuration();
    conf.set( "knox.webhdfs.context", "/gateway/default" );
    conf.set( "knox.webhdfs.password", "guest-password" );
    conf.set( "knox.webhdfs.verify.hostname", "false" );
    FileSystem fs = FileSystem.get( new URI( uri ), conf, "guest" );

    FileStatus[] status = fs.listStatus( new Path( uri + "/user" ) );
    for( int i = 0; i < status.length; i++ ) {
      System.out.println( status[ i ].getPath() );
    }
  }

}