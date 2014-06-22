
package com.google.refine.myDatabase;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import com.google.refine.model.Column;
import com.google.refine.model.Project;

import edu.pitt.sis.exp.colfusion.dao.DatabaseHandler;
import edu.pitt.sis.exp.colfusion.dao.MetadataDbHandler;
import edu.pitt.sis.exp.colfusion.dao.TargetDatabaseHandlerFactory;

/**
 * @author xxl
 *
 */
public class DatabaseOperation {

    /*
     * Rename operation
     * TODO: June 21, 2014 This function is not used for now,
     * but may be needed in the future, so keep it here
     */
//    public static void databaseRenameUpdate(String projectId, String newname, String oldname)
//            throws ClassNotFoundException, SQLException {
//        String myDriver = "com.mysql.jdbc.Driver";
//        String myDbURL = "jdbc:mysql://127.0.0.1:3306/colfusion";
//
//        String myUserName = "root";
//        String myUserPwd = "";
//
//        Class.forName(myDriver);
//
//        Connection conn2 = DriverManager.getConnection(myDbURL, myUserName, myUserPwd);
//        Statement ps2 = conn2.createStatement();
//
//        String getSidQuery = "select sid from colfusion_openrefine_project_map where projectId = '" + projectId + "'";
//        ResultSet rs2 = ps2.executeQuery(getSidQuery);
//        int mySid = 0;
//        while (rs2.next()) {
//            mySid = Integer.parseInt(rs2.getString(1));
//        }
//
//        String myDbURL1 = "jdbc:mysql://127.0.0.1:3306/colfusion_filetodb_" + mySid;
//
//        Connection conn3 = DriverManager.getConnection(myDbURL1, myUserName, myUserPwd);
//        Statement ps3 = conn3.createStatement();
//        /*
//         * Rename tables and temp_tables in colfusion_filetodb_'sid' database
//         */
//        String tableRename = String.format("RENAME TABLE %s TO %s", formatControl(oldname), formatControl(newname));
//        String tempTableRename = String.format("RENAME TABLE %s TO %s", "temp_" + formatControl(oldname), "temp_"
//                + formatControl(newname));
//        ps3.executeUpdate(tableRename);
//        ps3.executeUpdate(tempTableRename);
//
//        /*
//         * Rename tableName in colfusion_relationships
//         */
//        String relationshipsTableNameUpdated1 = String.format(
//                "UPDATE colfusion_relationships SET tableName1 = '%s' WHERE sid1 = %d", newname, mySid);
//        String relationshipsTableNameUpdated2 = String.format(
//                "UPDATE colfusion_relationships SET tableName2 = '%s' WHERE sid2 = %d", newname, mySid);
//        ps2.executeUpdate(relationshipsTableNameUpdated1);
//        ps2.executeUpdate(relationshipsTableNameUpdated2);
//
//        /*
//         * Rename tableName in colfusion_columnTableInfo
//         */
//        String getCid = String.format("SELECT cid FROM colfusion_dnameinfo WHERE sid = %d", mySid);
//        ResultSet rs3 = ps2.executeQuery(getCid);
//        while (rs3.next()) {
//            String columnTableInfoUpdate = String.format(
//                    "UPDATE colfusion_columnTableInfo SET tableName = '%s' WHERE cid = %d", newname,
//                    Integer.valueOf(rs3.getString("cid")));
//            Statement ps5 = conn2.createStatement();
//            ps5.executeUpdate(columnTableInfoUpdate);
//        }
//
//        /*
//         * Rename tableName in colfusion_openrefine_project_map
//         */
//        String projectMapTableNameUpdate = String.format(
//                "UPDATE colfusion_openrefine_project_map SET tableName = '%s' WHERE sid = %d", newname, mySid);
//        ps2.executeUpdate(projectMapTableNameUpdate);
//
//        /*
//         * Rename tableName in colfusion_table_change_log
//         */
//        String changeLogTableNameUpdate = String.format(
//                "UPDATE colfusion_table_change_log SET tableName = '%s' WHERE sid = %d", newname, mySid);
//        ps2.executeUpdate(changeLogTableNameUpdate);
//
//        if (ps2 != null) {
//            try {
//                ps2.close();
//            } catch (SQLException e) {
//                e.printStackTrace();
//            }
//        }
//
//        if (conn2 != null) {
//            try {
//                conn2.close();
//            } catch (SQLException e) {
//                e.printStackTrace();
//            }
//        }
//    }

    /*
     * Commented 06/03/2014 before trying to add temp tables to new created db
     * "colfusion_temptables" I'll use this database to store temp tables which
     * is used for the "Preview"
     */
    public static void databaseRowsColsUpdate(ArrayList<ArrayList<String>> rows, List<Column> columns, String name,
            long id)
            throws ClassNotFoundException, SQLException {

        MetadataDbHandler metadataDbHandler = TargetDatabaseHandlerFactory.getMetadataDbHandler();
        int sid = metadataDbHandler.getSid(String.valueOf(id));
        DatabaseHandler databaseHandler = TargetDatabaseHandlerFactory.getTargetDatabaseHandler(sid);
        String tableName = metadataDbHandler.getTableName(sid);

        /*
         * Refresh the operation start time
         */
        metadataDbHandler.refreshStartTime(sid, tableName);

        if (databaseHandler.tempTableExist(sid, tableName)) {
            databaseHandler.removeTable(sid, "temp_" + tableName);
        }

        if (columns.isEmpty()) {
            return;
        } else {
            String tableCreateQuery = "CREATE TABLE temp_" + tableName + " (";
            for (int i = 0; i < columns.size(); i++) {
                if (i < columns.size() - 1) {
                    tableCreateQuery += headlineControl(columns.get(i).getName()) + " VARCHAR(255), ";
                } else {
                    tableCreateQuery += headlineControl(columns.get(i).getName()) + " VARCHAR(255))";
                }
            }
            databaseHandler.createTempTable(tableCreateQuery, sid, tableName);

            if (rows.isEmpty()) {
                return;
            } else {
                String insertQuery = "INSERT INTO temp_" + tableName + " VALUES('";
                for (int j = 0; j < rows.size(); j++) {
                    for (int k = 0; k < columns.size(); k++) {
                        if (k < columns.size() - 1) {
                            insertQuery += rows.get(j).get(k) + "','";
                        } else {
                            insertQuery += rows.get(j).get(k) + "')";
                        }
                    }
                    insertQuery = insertQuery.replaceAll("\n", "");
                    databaseHandler.insertIntoTempTable(insertQuery, sid, tableName);
                    insertQuery = "INSERT INTO temp_" + tableName + " VALUES('";
                }
            }
        }

        /*
         * Update columns information in colfusion_dnameinfo
         */
        ArrayList<Integer> cids = metadataDbHandler.getCidsBySid(sid);
        // 1. Remove all the rows which's sid is Sid
        metadataDbHandler.deleteDnameinfoRowsBySid(sid);
        // 2. Add columns into the table
        for (int k = 0; k < columns.size(); k++) {
            String addRows = String
                    .format("INSERT INTO colfusion_dnameinfo VALUES(NULL, %d, '%s', 'String', NULL, NULL, NULL, '%s', b'0', NULL, NULL)",
                            sid, columns.get(k).getName(), columns.get(k).getName());
            metadataDbHandler.insertIntoDnameinfo(addRows, sid);
        }

        /*
         * Update columns information in colfusion_columnTableInfo
         */
        // 1. Remove all the rows which's cid is cid
        for (int j = 0; j < cids.size(); j++) {
            String getOriginalCids = String.format("DELETE FROM colfusion_columnTableInfo WHERE cid = %d", cids.get(j));
            metadataDbHandler.deleteColumninfoRowsByCid(getOriginalCids, cids.get(j));
        }
        // 2. Add all new added columns
        ArrayList<Integer> newCids = metadataDbHandler.getCidsBySid(sid);

        for (int n = 0; n < newCids.size(); n++) {
            String addColumns = String.format("INSERT INTO colfusion_columnTableInfo VALUES(%d, '%s')", newCids.get(n),
                    tableName);
            metadataDbHandler.insertIntoColumninfo(addColumns, tableName);
        }
    }

    /*
     * Remove all non-letter||number characters
     */
    public static String formatControl(String str) {
        String result = "";
        for (int i = 0; i < str.length(); i++) {
            if ((str.charAt(i) >= '0' && str.charAt(i) <= '9') || (str.charAt(i) >= 'a' && str.charAt(i) <= 'z')
                    || (str.charAt(i) >= 'A' && str.charAt(i) <= 'Z' || str.charAt(i) == '_')) {
                result += str.charAt(i);
            }
        }
        return result;
    }

    /*
     * Format columns' names
     */
    public static String headlineControl(String str) {
        String result = "`" + str + "`";
        return result;
    }

    /*
     * Get reordered rows in "ArrayList<ArrayList<String>>" format
     */
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
