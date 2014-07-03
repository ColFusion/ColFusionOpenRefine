
package com.google.refine.commands.colfusion;

import java.io.IOException;
import java.io.InputStream;
import java.io.Writer;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Properties;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.json.JSONException;
import org.json.JSONWriter;

import com.google.refine.ProjectManager;
import com.google.refine.commands.Command;
import com.google.refine.model.Project;

import edu.pitt.sis.exp.colfusion.dao.DatabaseHandler;
import edu.pitt.sis.exp.colfusion.dao.MetadataDbHandler;
import edu.pitt.sis.exp.colfusion.dao.TargetDatabaseHandlerFactory;

/**
 * @author xxl
 * 
 */
public class SaveProjectDataToDatabaseCommand extends Command {

    /*
     * By using this function, the
     * com.google.refine.myDatabase.DatabaseOperation will not be used anymore,
     * but just keep that file in case of future use.
     */
    @Override
    public void doPost(HttpServletRequest request, HttpServletResponse response)
            throws ServletException, IOException {

        Properties p = new Properties();
        String fileName = "/ColFusionOpenRefine.properties";
        InputStream in = SaveProjectDataToDatabaseCommand.class.getResourceAsStream(fileName);
        p.load(in);
        in.close();

        int lockTime = Integer.valueOf(p.getProperty("lock_time"));

        long projectId = Long.parseLong(request.getParameter("projectId"));
        String colfusionUserId = request.getParameter("colfusionUserId");
        int sid = -1;
        String tableName = "";

        String msg = "";

        MetadataDbHandler metadataDbHandler = TargetDatabaseHandlerFactory.getMetadataDbHandler();
        DatabaseHandler databaseHandler = null;

        try {
            sid = metadataDbHandler.getSid(String.valueOf(projectId));
            databaseHandler = TargetDatabaseHandlerFactory.getTargetDatabaseHandler(sid);
            tableName = metadataDbHandler.getTableName(sid);
        } catch (SQLException | ClassNotFoundException e1) {
            e1.printStackTrace();
        }
        
        try {
            if (metadataDbHandler.isTimeOutForCurrentUser(sid, tableName, Integer.valueOf(colfusionUserId), lockTime)) {
                msg = "Time is out, cannot save!";
            } else {
                // Get project
                Project project = ProjectManager.singleton.getProject(projectId);

                // Get column names from project
                ArrayList<String> columnNames = new ArrayList<>();
                for (String s : project.columnModel.getColumnNames()) {
                    columnNames.add(s);
                }

                // Get each row from project
                ArrayList<ArrayList<String>> rows = new ArrayList<>();
                rows = getReorderedRows(project);

                /*
                 * ****************Save data into database*****************
                 */
                // 0. Remove original table
                try {
                    databaseHandler.removeTable(sid, tableName);
                } catch (SQLException e2) {
                    e2.printStackTrace();
                }
                // 1. Create table
                String tableCreateQuery = "CREATE TABLE " + tableName + " (";
                for (int i = 0; i < columnNames.size(); i++) {
                    if (i < columnNames.size() - 1) {
                        tableCreateQuery += headlineControl(columnNames.get(i)) + " VARCHAR(255), ";
                    } else {
                        tableCreateQuery += headlineControl(columnNames.get(i)) + " VARCHAR(255))";
                    }
                }
                try {
                    databaseHandler.createOriginalTable(tableCreateQuery, sid, tableName);
                } catch (SQLException e1) {
                    e1.printStackTrace();
                }

                // 2. Insert rows into table
                String insertQuery = "INSERT INTO " + tableName + " VALUES('";
                for (int j = 0; j < rows.size(); j++) {
                    for (int k = 0; k < columnNames.size(); k++) {
                        if (k < columnNames.size() - 1) {
                            insertQuery += rows.get(j).get(k) + "','";
                        } else {
                            insertQuery += rows.get(j).get(k) + "')";
                        }
                    }
                    insertQuery = insertQuery.replaceAll("\n", "");
                    try {
                        databaseHandler.insertIntoTable(insertQuery, sid, tableName);
                    } catch (SQLException e) {
                        e.printStackTrace();
                    }
                    insertQuery = "INSERT INTO " + tableName + " VALUES('";
                }
                /*
                 * ********************************************************
                 */

                ArrayList<Integer> cids;
                try {
                    /*
                     * Update columns information in colfusion_dnameinfo
                     */
                    cids = metadataDbHandler.getCidsBySid(sid);
                    // 1. Remove all the rows which's sid is Sid
                    metadataDbHandler.deleteDnameinfoRowsBySid(sid);
                    // 2. Add columns into the table
                    for (int k = 0; k < columnNames.size(); k++) {
                        String addRows = String
                                .format("INSERT INTO colfusion_dnameinfo VALUES(NULL, %d, '%s', 'String', NULL, NULL, NULL, '%s', b'0', NULL, NULL)",
                                        sid, columnNames.get(k), columnNames.get(k));
                        metadataDbHandler.insertIntoDnameinfo(addRows, sid);
                    }

                    /*
                     * Update columns information in colfusion_columnTableInfo
                     */
                    // 1. Remove all the rows which's cid is cid
                    for (int j = 0; j < cids.size(); j++) {
                        String getOriginalCids = String.format("DELETE FROM colfusion_columnTableInfo WHERE cid = %d",
                                cids.get(j));
                        metadataDbHandler.deleteColumninfoRowsByCid(getOriginalCids, cids.get(j));
                    }
                    // 2. Add all new added columns
                    ArrayList<Integer> newCids = metadataDbHandler.getCidsBySid(sid);

                    for (int n = 0; n < newCids.size(); n++) {
                        String addColumns = String.format("INSERT INTO colfusion_columnTableInfo VALUES(%d, '%s')",
                                newCids.get(n), tableName);
                        metadataDbHandler.insertIntoColumninfo(addColumns, tableName);
                    }

                    msg = "Changes have been saved!";
                } catch (SQLException e1) {
                    e1.printStackTrace();
                }

                
            }
        } catch (NumberFormatException e) {
            e.printStackTrace();
        } catch (SQLException e) {
            e.printStackTrace();
        }
        
        Writer w = response.getWriter();
        JSONWriter writer = new JSONWriter(w);
        try {
            writer.object();

            writer.key("msg");
            writer.value(msg);
            writer.endObject();
        } catch (JSONException e) {
            throw new ServletException(e);
        } finally {
            w.flush();
            w.close();
        }
    }

    public static String headlineControl(String str) {
        String result = "`" + str + "`";
        return result;
    }

    public static ArrayList<ArrayList<String>> getReorderedRows(Project project) {
        ArrayList<ArrayList<String>> rows = new ArrayList<ArrayList<String>>();
        if (project.columnModel.columns.isEmpty() || project.rows.isEmpty()) {
            return rows;
        } else {
            for (int i = 0; i < project.rows.size(); i++) {
                rows.add(new ArrayList<String>());
                for (int j = 0; j < project.columnModel.columns.size(); j++) {
                    if (project.columnModel.columns.get(j).getCellIndex() > (project.rows.get(i).cells.size() - 1)) {
                        rows.get(i).add("");
                    } else {
                        if (project.rows.get(i).cells.get(project.columnModel.columns.get(j).getCellIndex()) == null) {
                            rows.get(i).add("");
                        } else {
                            rows.get(i).add(
                                    project.rows.get(i).cells.get(project.columnModel.columns.get(j).getCellIndex())
                                            .toString());
                        }
                    }
                }
            }
            return rows;
        }

    }
}
