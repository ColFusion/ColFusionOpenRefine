/*

Copyright 2014, xxl
All rights reserved.

Redistribution and use in source and binary forms, with or without
modification, are permitted provided that the following conditions are
met:

 * Redistributions of source code must retain the above copyright
notice, this list of conditions and the following disclaimer.
 * Redistributions in binary form must reproduce the above
copyright notice, this list of conditions and the following disclaimer
in the documentation and/or other materials provided with the
distribution.
 * Neither the name of Google Inc. nor the names of its
contributors may be used to endorse or promote products derived from
this software without specific prior written permission.

THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
"AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT
LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR
A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT
OWNER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT
LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE,           
DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY           
THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
(INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.

 */
package edu.pitt.sis.exp.colfusion.dao;

import java.io.IOException;
import java.io.InputStream;
import java.sql.SQLException;
import java.util.Properties;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * @author xxl
 *
 */
public class TargetDatabaseHandlerFactory {
    private static MetadataDbHandler metadataDbHandler;

    final static Logger logger = LoggerFactory.getLogger(TargetDatabaseHandlerFactory.class.getName());
    
    static {
        Properties p = new Properties();
        String fileName="/ColFusionOpenRefine.properties";
        InputStream in = TargetDatabaseHandlerFactory.class.getResourceAsStream(fileName);
        try {
            p.load(in);
            in.close();
        } catch (IOException e1) {
            e1.printStackTrace();
        }  
        String host = p.getProperty("mysql_host");
        int port = Integer.valueOf(p.getProperty("mysql_port"));
        String user = p.getProperty("mysql_user");
        String password = p.getProperty("mysql_password");
        String database = p.getProperty("mysql_database");
        
        //TODO:　Read host,port, etc. from config file and/or system properties
        DatabaseConnectionInfo connectioInfo = new DatabaseConnectionInfo(host, port, user, password, database);
        try {
            metadataDbHandler = new MetadataDbHandler(new MySQLDatabaseHandler(connectioInfo));
        } catch (ClassNotFoundException e) {
           logger.error("Couldn't intinialize meatdata db handler", e);
        }
    }
    
    public static DatabaseHandler getTargetDatabaseHandler(int sid) throws SQLException, ClassNotFoundException {
        DatabaseConnectionInfo connectioInfo = metadataDbHandler.getTargetDbConnectionInfo(sid);
        
       //TODO: this whole thing should be in a separate project and be shared with ColFusion
        return new MySQLDatabaseHandler(connectioInfo);
    }
    
    public static MetadataDbHandler getMetadataDbHandler() {
        return metadataDbHandler;
    }
}
