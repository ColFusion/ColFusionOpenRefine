
package com.google.refine.myDatabase;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

import com.google.refine.model.Column;
import com.google.refine.model.Project;
import com.google.refine.model.Row;
import com.google.refine.model.Cell;

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
    public static void databaseTableInput(ArrayList<ArrayList<String>> rows, List<Column> columns, String name, long id)
            throws ClassNotFoundException, SQLException {
        Class.forName(driver);
        Connection conn = DriverManager.getConnection(dbURL, userName, userPwd);
        Statement ps = conn.createStatement();
        String tableName = formatControl(name) + "_Pj" + id;
        String tableCreateQuery = "CREATE TABLE " + tableName + " (";
        for (int i = 0; i < columns.size(); i++) {
            if (i < columns.size() - 1)
                tableCreateQuery += headlineControl((String) columns.get(i).getName()) + " VARCHAR(255), ";
            else
                tableCreateQuery += headlineControl((String) columns.get(i).getName()) + " VARCHAR(255))";
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
    public static void databaseRenameUpdate(String projectId, String newname, String oldname)
            throws ClassNotFoundException, SQLException {
        Class.forName(driver);
        Connection conn = DriverManager.getConnection(dbURL, userName, userPwd);
        Statement ps = conn.createStatement();
        String tableOldName = formatControl(oldname) + "_Pj" + projectId;
        System.out.println("******test111111*****oldname: " + tableOldName);
        String tableNewName = formatControl(newname) + "_Pj" + projectId;
        System.out.println("******test111111*****newname: " + tableNewName);
        String tableRename = "EXEC sp_rename '" + tableOldName + "', '" + tableNewName + "'";
        System.out.println(tableRename);
        ps.executeUpdate(tableRename);
        System.out.println("********");
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

    // Cell content edit, Row & Col operations [i.e.] Col Addition / Col Removal
    // etc.
    /*
     * Original One
     */
//    public static void databaseRowsColsUpdate(ArrayList<ArrayList<String>> rows, List<Column> columns, String name,
//            long id)
//            throws ClassNotFoundException, SQLException {
//        Class.forName(driver);
//        Connection conn = DriverManager.getConnection(dbURL, userName, userPwd);
//        Statement ps = conn.createStatement();
//        String tableName = formatControl(name) + "_Pj" + id;
//        String tableDropQuery = "DROP TABLE " + tableName;
//        ps.executeUpdate(tableDropQuery);
//        if (columns.isEmpty()) {
//            return;
//        } else {
//            String tableCreateQuery = "CREATE TABLE " + tableName + " (";
//            for (int i = 0; i < columns.size(); i++) {
//                if (i < columns.size() - 1)
//                    tableCreateQuery += headlineControl((String) columns.get(i).getName()) + " VARCHAR(255), ";
//                else
//                    tableCreateQuery += headlineControl((String) columns.get(i).getName()) + " VARCHAR(255))";
//            }
//            ps.executeUpdate(tableCreateQuery);
//            if (rows.isEmpty()) {
//                return;
//            } else {
//                String insertQuery = "INSERT INTO " + tableName + " VALUES('";
//                for (int j = 0; j < rows.size(); j++) {
//                    for (int k = 0; k < columns.size(); k++) {
//                        if (k < columns.size() - 1) {
//                            insertQuery += rows.get(j).get(k) + "','";
//                        } else {
//                            insertQuery += rows.get(j).get(k) + "')";
//                        }
//                    }
//                    insertQuery = insertQuery.replaceAll("\n", "");
//                    ps.executeUpdate(insertQuery);
//                    insertQuery = "INSERT INTO " + tableName + " VALUES('";
//                }
//            }
//        }
//        if (ps != null) {
//            try {
//                ps.close();
//            } catch (SQLException e) {
//                e.printStackTrace();
//            }
//        }
//
//        if (conn != null) {
//            try {
//                conn.close();
//            } catch (SQLException e) {
//                e.printStackTrace();
//            }
//        }
//    }

    /* Commented 06/03/2014 before trying to add temp tables to new created db "colfusion_temptables"
     * I'll use this database to store temp tables which is used for the "Preview"
     */
    public static void databaseRowsColsUpdate(ArrayList<ArrayList<String>> rows, List<Column> columns, String name,
            long id)throws ClassNotFoundException, SQLException {
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
        
        String myDbURL1 = "jdbc:mysql://127.0.0.1:3306/colfusion_filetodb_" + mySid;
        Connection conn = DriverManager.getConnection(myDbURL1, myUserName, myUserPwd);
        Statement ps = conn.createStatement();
        
        String tableDropQuery = "DROP TABLE temp_" + tableName;
        if(tempTbExist(mySid, tableName))
            ps.executeUpdate(tableDropQuery);
        
        if (columns.isEmpty()) {
            return;
        } else {
            String tableCreateQuery = "CREATE TABLE temp_" + tableName + " (";
            for (int i = 0; i < columns.size(); i++) {
                if (i < columns.size() - 1)
                    tableCreateQuery += headlineControl((String) columns.get(i).getName()) + " VARCHAR(255), ";
                else
                    tableCreateQuery += headlineControl((String) columns.get(i).getName()) + " VARCHAR(255))";
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
    
//    public static void databaseRowsColsUpdate(ArrayList<ArrayList<String>> rows, List<Column> columns, String name,
//            long id)throws ClassNotFoundException, SQLException {
//        String myDriver = "com.mysql.jdbc.Driver";
//        String myDbURL = "jdbc:mysql://127.0.0.1:3306/colfusion"; // Connect
//                                                                                    // to
//                                                                                    // Server
//                                                                                    // &
//                                                                                    // DB连接服务器和数据库test
//        String myUserName = "root"; // UserName 用户名
//        String myUserPwd = "";
//        
//        Class.forName(myDriver);
//        
//        Connection conn2 = DriverManager.getConnection(myDbURL, myUserName, myUserPwd);
//        Statement ps2 = conn2.createStatement();
//        String getSidQuery = "select sid from colfusion_openrefine_project_map where projectId = '" + id + "'";
//        ResultSet rs2 = ps2.executeQuery(getSidQuery);
//        int mySid = 0;
//        while(rs2.next()) {
//            mySid = Integer.parseInt(rs2.getString(1));
//        }
//        
//        Connection conn1 = DriverManager.getConnection(myDbURL, myUserName, myUserPwd);
//        Statement ps1 = conn1.createStatement();
//        String getTableNameQuery = "select tableName from colfusion_columntableinfo where cid = (select cid from colfusion_dnameinfo where sid = " + mySid + " limit 1)";
//        ResultSet rs1 = ps1.executeQuery(getTableNameQuery);
//        String tableName = "";
//        while(rs1.next()) {
//            tableName = rs1.getString(1);
//        }
//        
//        String myDbURL1 = "jdbc:mysql://127.0.0.1:3306/colfusion_temptables";
//        Connection conn = DriverManager.getConnection(myDbURL1, myUserName, myUserPwd);
//        Statement ps = conn.createStatement();
//        String tableDropQuery = "DROP TABLE IF EXISTS tempTb_" + mySid + "_" + tableName;
//        ps.executeUpdate(tableDropQuery);
//        if (columns.isEmpty()) {
//            return;
//        } else {
//            String tableCreateQuery = "CREATE TABLE tempTb_" + mySid + "_" + tableName + " (";
//            for (int i = 0; i < columns.size(); i++) {
//                if (i < columns.size() - 1)
//                    tableCreateQuery += headlineControl((String) columns.get(i).getName()) + " VARCHAR(255), ";
//                else
//                    tableCreateQuery += headlineControl((String) columns.get(i).getName()) + " VARCHAR(255))";
//            }
//            ps.executeUpdate(tableCreateQuery);
//            if (rows.isEmpty()) {
//                return;
//            } else {
//                String insertQuery = "INSERT INTO tempTb_" + mySid + "_" + tableName + " VALUES('";
//                for (int j = 0; j < rows.size(); j++) {
//                    for (int k = 0; k < columns.size(); k++) {
//                        if (k < columns.size() - 1) {
//                            insertQuery += rows.get(j).get(k) + "','";
//                        } else {
//                            insertQuery += rows.get(j).get(k) + "')";
//                        }
//                    }
//                    insertQuery = insertQuery.replaceAll("\n", "");
//                    ps.executeUpdate(insertQuery);
//                    insertQuery = "INSERT INTO tempTb_" + mySid + "_" + tableName + " VALUES('";
//                }
//            }
//        }
//        if (ps != null) {
//            try {
//                ps.close();
//            } catch (SQLException e) {
//                e.printStackTrace();
//            }
//        }
//
//        if (conn != null) {
//            try {
//                conn.close();
//            } catch (SQLException e) {
//                e.printStackTrace();
//            }
//        }
//    }
    
    
    public static boolean tempTbExist(int sid, String tableName) throws SQLException {
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
if(result.next())
        return true;
else {
                return false;
        }
}
    
    
    // Format String
    public static String formatControl(String str) {
        String result = "";
        for (int i = 0; i < str.length(); i++) {
            if ((str.charAt(i) >= '0' && str.charAt(i) <= '9') || (str.charAt(i) >= 'a' && str.charAt(i) <= 'z')
                    || (str.charAt(i) >= 'A' && str.charAt(i) <= 'Z' || str.charAt(i) == '_')) result += str.charAt(i);
        }
        return result;
    }

    // Format Col's name
    public static String headlineControl(String str) {
        String result = "Column_";
        for (int i = 0; i < str.length(); i++) {
            if ((str.charAt(i) >= '0' && str.charAt(i) <= '9') || (str.charAt(i) >= 'a' && str.charAt(i) <= 'z')
                    || (str.charAt(i) >= 'A' && str.charAt(i) <= 'Z' || str.charAt(i) == '_')) result += str.charAt(i);
        }
        return result;
    }

    // Table drop
    public static void dababaseTableDrop(String name, long id)
            throws ClassNotFoundException, SQLException {
        Class.forName(driver);
        Connection conn = DriverManager.getConnection(dbURL, userName, userPwd);
        Statement ps = conn.createStatement();
        String tableName = formatControl(name) + "_Pj" + id;
        String tableDropQuery = "DROP TABLE " + tableName;
        ps.executeUpdate(tableDropQuery);
    }

    // Get reordered rows in "ArrayList<ArrayList<String>>" format
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
