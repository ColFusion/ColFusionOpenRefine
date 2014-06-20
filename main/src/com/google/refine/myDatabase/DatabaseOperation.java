
package com.google.refine.myDatabase;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import com.google.refine.model.Column;
import com.google.refine.model.Project;

public class DatabaseOperation {

    static String driver = "com.microsoft.sqlserver.jdbc.SQLServerDriver";
    static String dbURL = "jdbc:sqlserver://localhost:50076; DatabaseName=xxl"; // Connect
                                                                                // to
                                                                                // Server
                                                                                // &
                                                                                // DB连接服务器和数据库test
    static String userName = "xxl"; // UserName 用户名
    static String userPwd = "wwpswwpsxx"; // Pwd 密码

    /*
     * Create tables when projects are created
     */
    public static void databaseTableInput(final ArrayList<ArrayList<String>> rows, final List<Column> columns, final String name, final long id)
            throws ClassNotFoundException, SQLException {
        Class.forName(driver);
        Connection conn = DriverManager.getConnection(dbURL, userName, userPwd);
        Statement ps = conn.createStatement();
        String tableName = formatControl(name) + "_Pj" + id;
        String tableCreateQuery = "CREATE TABLE " + tableName + " (";
        for (int i = 0; i < columns.size(); i++) {
            if (i < columns.size() - 1) {
                tableCreateQuery += headlineControl(columns.get(i).getName()) + " VARCHAR(255), ";
            } else {
                tableCreateQuery += headlineControl(columns.get(i).getName()) + " VARCHAR(255))";
            }
        }
        ps.executeUpdate(tableCreateQuery);
        String insertQuery = "INSERT INTO " + tableName + " VALUES('";
        for (int j = 0; j < rows.size(); j++) {
            for (int k = 0; k < columns.size(); k++) {
                if (k < columns.size() - 1) {
                    insertQuery += rows.get(j).get(k) + "','";
                } else {
                    insertQuery += rows.get(j).get(k) + "')";
                }
            }
            insertQuery = insertQuery.replaceAll("\n", "");
            ps.executeUpdate(insertQuery);
            insertQuery = "INSERT INTO " + tableName + " VALUES('";
        }
        if (ps != null) {
            try {
                ps.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }

        if (conn != null) {
            try {
                conn.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
    }

    // Rename operation
    // TODO:
    // Not sure if needs to be done, because there are too many tables in DB that contains "tableName" column,
    // if tableName is changed, too many tables needs to be changed as well
    public static void databaseRenameUpdate(final String projectId, final String newname, final String oldname)
            throws ClassNotFoundException, SQLException {
        String myDriver = "com.mysql.jdbc.Driver";
        String myDbURL = "jdbc:mysql://127.0.0.1:3306/colfusion";
        
        String myUserName = "root";
        String myUserPwd = "";
        
        Class.forName(myDriver);
        
        Connection conn2 = DriverManager.getConnection(myDbURL, myUserName, myUserPwd);
        Statement ps2 = conn2.createStatement();
        
        String getSidQuery = "select sid from colfusion_openrefine_project_map where projectId = '" + projectId + "'";
        ResultSet rs2 = ps2.executeQuery(getSidQuery);
        int mySid = 0;
        while(rs2.next()) {
            mySid = Integer.parseInt(rs2.getString(1));
        }
        
        
        
        String myDbURL1 = "jdbc:mysql://127.0.0.1:3306/colfusion_filetodb_" + mySid;

        
        Connection conn3 = DriverManager.getConnection(myDbURL1, myUserName, myUserPwd);
        Statement ps3 = conn3.createStatement();
        /*
         * Rename tables and temp_tables in colfusion_filetodb_'sid' database
         */
        String tableRename = String.format("RENAME TABLE %s TO %s", formatControl(oldname), formatControl(newname));
        String tempTableRename = String.format("RENAME TABLE %s TO %s", "temp_" + formatControl(oldname), "temp_" + formatControl(newname));
        ps3.executeUpdate(tableRename);
        ps3.executeUpdate(tempTableRename);
        
        /*
         * Rename tableName in colfusion_relationships
         */
        String relationshipsTableNameUpdated1 = String.format("UPDATE colfusion_relationships SET tableName1 = '%s' WHERE sid1 = %d", newname, mySid);
        String relationshipsTableNameUpdated2 = String.format("UPDATE colfusion_relationships SET tableName2 = '%s' WHERE sid2 = %d", newname, mySid);
        ps2.executeUpdate(relationshipsTableNameUpdated1);
        ps2.executeUpdate(relationshipsTableNameUpdated2);
        
        /*
         * Rename tableName in colfusion_columnTableInfo
         */
        String getCid = String.format("SELECT cid FROM colfusion_dnameinfo WHERE sid = %d", mySid);
        ResultSet rs3 = ps2.executeQuery(getCid);
        while(rs3.next()) {
            String columnTableInfoUpdate = String.format("UPDATE colfusion_columnTableInfo SET tableName = '%s' WHERE cid = %d", newname, Integer.valueOf(rs3.getString("cid")));
            Statement ps5 = conn2.createStatement();
            ps5.executeUpdate(columnTableInfoUpdate);
        }
        
        /*
         * Rename tableName in colfusion_openrefine_project_map
         */
        String projectMapTableNameUpdate = String.format("UPDATE colfusion_openrefine_project_map SET tableName = '%s' WHERE sid = %d", newname, mySid);
        ps2.executeUpdate(projectMapTableNameUpdate);
        
        /*
         * Rename tableName in colfusion_table_change_log
         */
        String changeLogTableNameUpdate = String.format("UPDATE colfusion_table_change_log SET tableName = '%s' WHERE sid = %d", newname, mySid);
        ps2.executeUpdate(changeLogTableNameUpdate);
        
        
        if (ps2 != null) {
            try {
                ps2.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }

        if (conn2 != null) {
            try {
                conn2.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
    }


    /* Commented 06/03/2014 before trying to add temp tables to new created db "colfusion_temptables"
     * I'll use this database to store temp tables which is used for the "Preview"
     */
    public static void databaseRowsColsUpdate(final ArrayList<ArrayList<String>> rows, final List<Column> columns, final String name,
            final long id)throws ClassNotFoundException, SQLException {
        String myDriver = "com.mysql.jdbc.Driver";
        String myDbURL = "jdbc:mysql://127.0.0.1:3306/colfusion"; // Connect
                                                                                    // to
                                                                                    // Server
                                                                                    // &
                                                                                    // DB连接服务器和数据库test
        String myUserName = "root"; // UserName 用户名
        String myUserPwd = "";
        
        Class.forName(myDriver);
        
        Connection conn2 = DriverManager.getConnection(myDbURL, myUserName, myUserPwd);
        Statement ps2 = conn2.createStatement();
        String getSidQuery = "select sid from colfusion_openrefine_project_map where projectId = '" + id + "'";
        ResultSet rs2 = ps2.executeQuery(getSidQuery);
        int mySid = 0;
        while(rs2.next()) {
            mySid = Integer.parseInt(rs2.getString(1));
        }
        
        Connection conn1 = DriverManager.getConnection(myDbURL, myUserName, myUserPwd);
        Statement ps1 = conn1.createStatement();
        String getTableNameQuery = "select tableName from colfusion_columntableinfo where cid = (select cid from colfusion_dnameinfo where sid = " + mySid + " limit 1)";
        ResultSet rs1 = ps1.executeQuery(getTableNameQuery);
        String tableName = "";
        while(rs1.next()) {
            tableName = rs1.getString(1);
        }
        
        /*
         * Refresh the operation start time
         */
        SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        String currentTime = format.format(new Date());
        String refreshStartTime = String.format("UPDATE colfusion_table_change_log SET startChangeTime = '%s' WHERE sid = %d and tableName = '%s'", currentTime, mySid, tableName);
        ps1.executeUpdate(refreshStartTime);
        
        
        
        String myDbURL1 = "jdbc:mysql://127.0.0.1:3306/colfusion_filetodb_" + mySid;
        Connection conn = DriverManager.getConnection(myDbURL1, myUserName, myUserPwd);
        Statement ps = conn.createStatement();
        
        String tableDropQuery = "DROP TABLE temp_" + tableName;
        if(tempTbExist(mySid, tableName)) {
            ps.executeUpdate(tableDropQuery);
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
            ps.executeUpdate(tableCreateQuery);
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
                    ps.executeUpdate(insertQuery);
                    insertQuery = "INSERT INTO temp_" + tableName + " VALUES('";
                }
            }
        }
        
        /*
         * Update columns information in colfusion_dnameinfo
         */
        ArrayList<Integer> cids = new ArrayList<>();
        String getCids = String.format("SELECT cid FROM colfusion_dnameinfo WHERE sid = %d", mySid);
        ResultSet rs3 = ps2.executeQuery(getCids);
        while(rs3.next()) {
            cids.add(rs3.getInt("cid"));
        }
            // 1. Remove all the rows which's sid is mySid
        String removeRows = String.format("DELETE FROM colfusion_dnameinfo WHERE sid = %d", mySid);
        ps2.executeUpdate(removeRows);
            // 2. Add columns into the table
        for(int k = 0; k < columns.size(); k++) {
            String addRows = String.format("INSERT INTO colfusion_dnameinfo VALUES(NULL, %d, '%s', 'String', NULL, NULL, NULL, '%s', b'0', NULL, NULL)", mySid, columns.get(k).getName(), columns.get(k).getName());
            ps2.executeUpdate(addRows);
        }
        
        /*
         * Update columns information in colfusion_columnTableInfo
         */
            // 1. Remove all the rows which's cid is cid
        for(int j = 0; j < cids.size(); j++) {
            String getOriginalCids = String.format("DELETE FROM colfusion_columnTableInfo WHERE cid = %d", cids.get(j));
            ps2.executeUpdate(getOriginalCids);
        }
            // 2. Add all new added columns
        ArrayList<Integer> newCids = new ArrayList<>();
        String getNewCids = String.format("SELECT cid FROM colfusion_dnameinfo WHERE sid = %d", mySid);
        ResultSet rs4 = ps2.executeQuery(getNewCids);
        while(rs4.next()) {
            newCids.add(rs4.getInt("cid"));
        }
        
        for(int n = 0; n < newCids.size(); n++) {
            String addColumns = String.format("INSERT INTO colfusion_columnTableInfo VALUES(%d, '%s')", newCids.get(n), tableName);
            ps2.executeUpdate(addColumns);
        }
        
        
        
        if (ps != null) {
            try {
                ps.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }

        if (conn != null) {
            try {
                conn.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
    }
    

    
    
    public static boolean tempTbExist(final int sid, final String tableName)
            throws SQLException {
        String driver = "com.mysql.jdbc.Driver";
        String dbURL = "jdbc:mysql://127.0.0.1:3306/colfusion_filetodb_" + sid; // Connect
                                                                                // to
                                                                                // Server
                                                                                // &
                                                                                // DB连接服务器和数据库test
        String userName = "root"; // UserName 用户名
        String userPwd = ""; // Pwd 密码

        try {
            Class.forName(driver);
        } catch (ClassNotFoundException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        Connection conn = DriverManager.getConnection(dbURL, userName, userPwd);

        String query = "SHOW TABLES LIKE 'temp_" + tableName + "'";
        Statement ps = conn.createStatement();
        ResultSet result = ps.executeQuery(query);
        if (result.next()) {
            return true;
        } else {
            return false;
        }
    }
    
    
    // Format String
    // Remove all non-letter||number characters
    public static String formatControl(final String str) {
        String result = "";
        for (int i = 0; i < str.length(); i++) {
            if ((str.charAt(i) >= '0' && str.charAt(i) <= '9') || (str.charAt(i) >= 'a' && str.charAt(i) <= 'z')
                    || (str.charAt(i) >= 'A' && str.charAt(i) <= 'Z' || str.charAt(i) == '_')) {
                result += str.charAt(i);
            }
        }
        return result;
    }

    // Format Col's name
    // In case of "NULL" String
    public static String headlineControl(final String str) {
        String result = "`" + str + "`";
        return result;
    }

    // Table drop
    // For now, we don't need this function in Col*Fusion project
    public static void dababaseTableDrop(final String name, final long id)
            throws ClassNotFoundException, SQLException {
        Class.forName(driver);
        Connection conn = DriverManager.getConnection(dbURL, userName, userPwd);
        Statement ps = conn.createStatement();
        String tableName = formatControl(name) + "_Pj" + id;
        String tableDropQuery = "DROP TABLE " + tableName;
        ps.executeUpdate(tableDropQuery);
    }

    // Get reordered rows in "ArrayList<ArrayList<String>>" format
    public static ArrayList<ArrayList<String>> getReorderedRows(final Project project) {
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
