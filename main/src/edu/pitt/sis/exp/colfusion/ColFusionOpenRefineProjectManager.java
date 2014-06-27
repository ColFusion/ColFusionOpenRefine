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

package edu.pitt.sis.exp.colfusion;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Properties;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.refine.ProjectManager;
import com.google.refine.ProjectMetadata;
import com.google.refine.io.FileProjectManager;
import com.google.refine.model.Cell;
import com.google.refine.model.Column;
import com.google.refine.model.ModelException;
import com.google.refine.model.Project;
import com.google.refine.model.Row;

import edu.pitt.sis.exp.colfusion.dao.DatabaseHandler;
import edu.pitt.sis.exp.colfusion.dao.MetadataDbHandler;
import edu.pitt.sis.exp.colfusion.dao.MySQLDatabaseHandler;
import edu.pitt.sis.exp.colfusion.dao.TargetDatabaseHandlerFactory;

/**
 * @author xxl
 * 
 */
public class ColFusionOpenRefineProjectManager {

    final static Logger logger = LoggerFactory.getLogger(ColFusionOpenRefineProjectManager.class.getName());
    
    public String createProjectToOpenRefine(int sid, String tableName)
            throws ClassNotFoundException, SQLException, IOException {
        ProjectManager.singleton.setBusy(true);
        String url = "";

        Properties p = new Properties();
        String fileName="/ColFusionOpenRefine.properties";
        InputStream in = ColFusionOpenRefineProjectManager.class.getResourceAsStream(fileName);
        p.load(in);  
        in.close();
        
        String fileDir = p.getProperty("file_dir");
        String openrefineUrl = p.getProperty("openrefine_link");
        System.out.println("************************************************");
        System.out.println("OpenRefineUrl: " + openrefineUrl);
        System.out.println("************************************************");
        
        // DatabaseConnectionInfo databaseConnectionInfo = new
        // DatabaseConnectionInfo("127.0.0.1", 3306, "root", "",
        // "colfusion_filetodb_")
        DatabaseHandler databaseHandler = TargetDatabaseHandlerFactory.getTargetDatabaseHandler(sid);

        MetadataDbHandler metadataDbHandler = TargetDatabaseHandlerFactory.getMetadataDbHandler();

        try {
            /*
             * If there is no temp table, then backup original table to a temp_
             * table
             */
            if (databaseHandler.tempTableExist(sid, tableName)) {
                databaseHandler.removeTable(sid, "temp_" + tableName);
                databaseHandler.backupOriginalTable(sid, tableName);
            } else {
                databaseHandler.backupOriginalTable(sid, tableName);
            }
            /*
             * Create a new project for the table in the database
             */
            String projectLink = "";

            if (metadataDbHandler.isMapExist(sid, tableName)) {
                projectLink = metadataDbHandler.getProjectId(sid, tableName);
            } else {
                Project project = new Project();
                ProjectMetadata ppm = new ProjectMetadata();

                ppm.setName(tableName);
                ppm.setEncoding("UTF-8");
                this.setProject(sid, project, databaseHandler, metadataDbHandler);

              //  dataDir); //   String dataDir = Configurations.get("refine.data_dir");

                File dir = new File(fileDir);

                
                FileProjectManager.initialize(dir);

                project.update();

                FileProjectManager.singleton.registerProject(project, ppm);
                FileProjectManager.singleton.ensureProjectSaved(project.id);
                /*
                 * ********************************************
                 * The following two lines solve the problem:
                 * 
                 * "Only the first time after rerun ColFusionServer, click the "
                 * Edit " button can create a project correctly" I guess the
                 * reason is because without the "save()", some stuff will exist
                 * in cache so that the new created project cannot be stored
                 * correctly
                 */
                project.dispose();
                FileProjectManager.singleton.save(true);
                /*
                 * ********************************************
                 */

                metadataDbHandler.saveRelation(project.id, sid, tableName);

                projectLink = project.id + "";
            }

            url = openrefineUrl + projectLink;

        } catch (Exception e) {

        } finally {
            ProjectManager.singleton.setBusy(false);
        }
        return url;
    }

    private void setProject(int sid, Project project, DatabaseHandler dbHandler,
            MetadataDbHandler metadataDbHandler)
            throws SQLException {
        // Get and set column names

        setProjectCol(project, metadataDbHandler.getColumnNames(sid));

        // Get and set rows
        String tableName = metadataDbHandler.getTableName(sid);

        System.out.println(String.format("Table name in ColFusionOpenRefineProjectManager.setProject method is %s", tableName)); 
        
        int colCount = dbHandler.getColCount(sid, tableName);
        
        System.out.println(String.format("colCount in ColFusionOpenRefineProjectManager.setProject method is %d", colCount)); 

        setProjectRow(project, dbHandler.getRows(tableName, colCount));
    }

    public static void setProjectCol(Project project, ArrayList<String> columnNames) {
        for (int i = 0; i < columnNames.size(); i++) {
            Column column = new Column(i, columnNames.get(i));
            try {
                project.columnModel.addColumn(i, column, true);
            } catch (ModelException e) {
                e.printStackTrace();
            }
        }
    }

    public static void setProjectRow(Project project, ArrayList<ArrayList<String>> rows) {
        for (int j = 0; j < rows.size(); j++) {
            Row row = new Row(rows.get(j).size());
            for (int k = 0; k < rows.get(j).size(); k++) {
                Cell cell = new Cell(rows.get(j).get(k), null);
                
                System.out.println(String.format("Setting row %d, cell %k to value %s", j, k, cell.value.toString())); 
                
                row.setCell(k, cell);
                
            }
            project.rows.add(row);
        }
    }

}
