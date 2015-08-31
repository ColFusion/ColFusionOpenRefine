/*

Copyright 2010, Google Inc.
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

package com.google.refine.commands.workspace;

import java.io.IOException;
import java.net.URI;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.json.JSONException;
import org.json.JSONWriter;


import com.google.refine.ProjectManager;
import com.google.refine.ProjectMetadata;
import com.google.refine.commands.Command;
import com.google.refine.model.Cell;
import com.google.refine.model.Column;
import com.google.refine.model.ModelException;
import com.google.refine.model.Project;
import com.google.refine.model.Row;

public class GetAllProjectMetadataCommand extends Command {
//    public static int sid = 0;
//    public static String tableName = "";
    
//    public static int test = Values.myValue;
    
    @Override
    public void doGet(HttpServletRequest request, HttpServletResponse response)
            throws ServletException, IOException {
        
        try {
            response.setCharacterEncoding("UTF-8");
            response.setHeader("Content-Type", "application/json");
            
            JSONWriter writer = new JSONWriter(response.getWriter());
            Properties options = new Properties();
            
            writer.object();
            writer.key("projects");
                writer.object();
//                Map<Long, ProjectMetadata> mm = ProjectManager.singleton.getAllProjectMetadata();
//                
//                ProjectMetadata[] alextest = new ProjectMetadata[60];
//                Long[] alexnum = new Long[60];
//                int cc = 0;
//                int dd = 0;
//                for (Entry<Long,ProjectMetadata> e : mm.entrySet()) {
//                    alextest[cc++] = e.getValue();
//                    alexnum[dd++] = e.getKey() + 1;
//                }
                
                
//                ProjectMetadata alexmeta = new ProjectMetadata();
//                Project alexpro = new Project();
//                ProjectManager.singleton.registerProject(alexpro, alexmeta);
//                ProjectManager.singleton.ensureProjectSaved(alexpro.id);
//                System.out.println("=========>" + alexpro.id);
                
                
//                Long[] alexlong = new Long[9];
//                int count = 0;
//                Map<Long, ProjectMetadata> mmm = ProjectManager.singleton.getAllProjectMetadata();
//                for (Entry<Long,ProjectMetadata> e : mmm.entrySet()) {
//                    if(count >8)
//                        break;
//                    else {
//                        alexlong[count++] = e.getKey();
//                    }
//                }
//                for(int i = 0; i < 9; i++) {
//                ProjectManager.singleton.deleteProject(Long.parseLong("2135065729448"));
//                }
                /*
                 * ***************************************************
                 * Added by Alex
                 */
//                if(sid != 0 && tableName != "") {
//                Project newProject = new Project();
//                ProjectMetadata ppm = new ProjectMetadata();
//                int sid = 1711;
//                
//                ppm.setName("This_is_a_test113");
//                ppm.setEncoding("UTF-8");
//                setProject(sid, newProject);
////                setMyProjectCol(newProject);
////                setMyProjectRow(newProject);
//                ProjectManager.singleton.registerProject(newProject, ppm);
//                ProjectManager.singleton.ensureProjectSaved(newProject.id);
//                
//                System.out.println("***********************************");
////                System.out.println(test);
////                System.out.println("sid is: " + sid + " tableName is: " + tableName);
//                System.out.println("***********************************");
//                
//                saveRelation(newProject.id, sid, "sheet1");
//                } else {
//                    System.out.println("***********************************");
//                    System.out.println("Failed! sid is: " + sid + " tableName is: " + tableName);
//                    System.out.println("***********************************");
                    
//                }
                
//                String url = "http://127.0.0.1:3333/project?project=" + newProject.id;
//                URI uri = null;
//                try {
//                        uri = new java.net.URI(url);  
//                } catch (Exception e) {
//                        logger.info("Failed to load webpage!");
//                }
//                try {
//                        java.awt.Desktop.getDesktop().browse(uri);      
//                } catch (Exception e) {
//                        logger.info("Failed to open browser");
//                }
                
                /*
                 * Above
                 * ****************************************************
                 */
                Map<Long, ProjectMetadata> m = ProjectManager.singleton.getAllProjectMetadata();
                
                
                
                for (Entry<Long,ProjectMetadata> e : m.entrySet()) {
                    ProjectMetadata pm = e.getValue();
                    if (pm != null) {
                        writer.key(e.getKey().toString());
                        e.getValue().write(writer, options);
                    }
                }
                writer.endObject();
            writer.endObject();
        } catch (JSONException e) {
            respondException(response, e);
        }
//        catch (SQLException e) {
//            // TODO Auto-generated catch block
//            e.printStackTrace();
//        }
    }
    
    public void setMyProjectCol(Project newProject) {
        for(int i = 0; i < 3; i++) {
            Column column = new Column(i, "a" + i);
            try {
                    newProject.columnModel.addColumn(i, column, true);
            } catch (ModelException e) {
                    // TODO Auto-generated catch block
                    e.printStackTrace();
            }
        }
    }
    
    public void setMyProjectRow(Project newProject) {
        for(int j = 0; j < 3; j++) {
            Row row = new Row(3);
            for(int k = 0; k < 3; k++) {
                    Cell cell = new Cell("b"+k, null);
                    row.setCell(k, cell);
            }
            newProject.rows.add(row);
        }
    }
    public void saveRelation(long projectId, int sid, String tableName) throws SQLException {
        String driver = "com.mysql.jdbc.Driver";
        String dbURL = "jdbc:mysql://127.0.0.1:3306/colfusion"; 
        String userName = "root"; // UserName 用户名
        String userPwd = ""; // Pwd 密码
        
        Connection conn = null;
        try {
                        Class.forName(driver);
                } catch (ClassNotFoundException e) {
                        // TODO Auto-generated catch block
                        e.printStackTrace();
                }
        conn = DriverManager.getConnection(dbURL, userName, userPwd);
        
        String insertQuery = String.format("insert into colfusion_openrefine_project_map values('%d', %d, '%s')", projectId, sid, tableName);
        Statement ps = conn.createStatement();
        ps.executeUpdate(insertQuery);
    }
    public void setProject(int sid, Project project) throws SQLException {
        String driver = "com.mysql.jdbc.Driver";
        String dbURL = "jdbc:mysql://127.0.0.1:3306/colfusion"; 
        String userName = "root"; // UserName 用户名
        String userPwd = ""; // Pwd 密码
        
        Connection conn = null;
        try {
                        Class.forName(driver);
                } catch (ClassNotFoundException e) {
                        // TODO Auto-generated catch block
                        e.printStackTrace();
                }
        conn = DriverManager.getConnection(dbURL, userName, userPwd);
        
        // Get and set column names
        String colQuery = "select dname_chosen from colfusion_dnameinfo where sid = " + sid;
        Statement ps = conn.createStatement();
        ResultSet rs = ps.executeQuery(colQuery);
        ArrayList<String> columnNames = new ArrayList<String>();
        while(rs.next()) {
                columnNames.add(rs.getString("dname_chosen"));
        }
        setProjectCol(project, columnNames);
        
        // Get and set rows
        String rowQuery = "select tableName from colfusion_columntableinfo where cid = (select cid from colfusion_dnameinfo limit 1)";
        Statement ps1 = conn.createStatement();
        
        ResultSet newRs = ps1.executeQuery(rowQuery);
        String tableName = "";
        while(newRs.next()) {
            tableName = newRs.getString("tableName");
        }
        
        
        
        Connection conn1 = null;
        String dbURL1 = "jdbc:mysql://127.0.0.1:3306/colfusion_filetodb_" + sid;
        conn1 = DriverManager.getConnection(dbURL1, userName, userPwd);
        String rowQuery1 = "select * from " + tableName;
        Statement ps2 = conn1.createStatement();
        Statement ps3 = conn1.createStatement();
        ResultSet rs1 = ps2.executeQuery(rowQuery1);
        
        ResultSet rsCount = ps3.executeQuery("select count(*) from information_schema.columns where table_schema='colfusion_filetodb_" + sid + "' and table_name='" + tableName + "'");
        int colCount = 0;
        while(rsCount.next()) {
                colCount = Integer.parseInt(rsCount.getString(1));
        }
        
        ArrayList<ArrayList<String>> rows = new ArrayList<ArrayList<String>>();
   
        while(rs1.next()) {
            int colIndex = 1;
            ArrayList<String> temp = new ArrayList<String>();
            while(colIndex <= colCount) {
                temp.add(rs1.getString(colIndex));
                colIndex++;
            }
            rows.add(temp);
        }
        setProjectRow(project, rows);
    }
    
    public void setProjectCol(Project project, ArrayList<String> columnNames) {
        for(int i = 0; i < columnNames.size(); i++) {
            Column column = new Column(i, columnNames.get(i));
            try {
                    project.columnModel.addColumn(i, column, true);
            } catch (ModelException e) {
                    // TODO Auto-generated catch block
                    e.printStackTrace();
            }
        }
    }
    
    public void setProjectRow(Project project, ArrayList<ArrayList<String>> rows) {
        for(int j = 0; j < rows.size(); j++) {
            Row row = new Row(rows.get(j).size());
            for(int k = 0; k < rows.get(j).size(); k++) {
                    Cell cell = new Cell(rows.get(j).get(k), null);
                    row.setCell(k, cell);
            }
            project.rows.add(row);
    }
    }
}