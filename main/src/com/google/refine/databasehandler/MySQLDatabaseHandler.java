
package com.google.refine.databasehandler;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import com.google.refine.model.Project;

public class MySQLDatabaseHandler {

    private String host;
    private int port;
    private String user;
    private String password;
    private String database;

    protected Connection connection;

    public MySQLDatabaseHandler(final String host, final int port, final String user, final String password,
            final String database) throws Exception {
        setHost(host);
        setPort(port);
        setUser(user);
        setPassword(password);
        setDatabase(database);

        try {
            Class.forName("com.mysql.jdbc.Driver");
        } catch (ClassNotFoundException e) {
            // TODO: Add sth here
        }

        openConnection(getConnectionString());
    }

    protected void openConnection(final String connectionString)
            throws Exception {
        try {
            connection = DriverManager.getConnection(connectionString, getUser(), getPassword());

        } catch (SQLException e) {
            // TODO: Add sth here
        }
    }

    public String getConnectionString() {
        return String.format("jdbc:mysql://%s:%d/%s", getHost(), getPort(), getDatabase());
    }

    public String getHost() {
        return host;
    }

    public void setHost(final String host) {
        this.host = host;
    }

    public int getPort() {
        return port;
    }

    public void setPort(final int port) {
        this.port = port;
    }

    public String getUser() {
        return user;
    }

    public void setUser(final String user) {
        this.user = user;
    }

    public String getPassword() {
        return password;
    }

    public void setPassword(final String password) {
        this.password = password;
    }

    public String getDatabase() {
        return database;
    }

    public void setDatabase(final String database) {
        this.database = database;
    }

    /*
     * The followings functions are used in "command" files
     */
    
//    public int getOperatingUserId(int sid, String tableName) {
//        int uid = 0;
//        try {
//            String query = "SELECT operatedUser FROM colfusion_table_change_log WHERE endChangeTime IS NULL AND sid = " + sid + " AND tableName = '" + tableName + "'";
//            Statement ps = connection.createStatement();
//            ResultSet result = ps.executeQuery(query);
//            while (result.next()) {
//                uid = Integer.valueOf(result.getString("operatedUser"));
//            }
//        } catch (SQLException e) {
//            e.printStackTrace();
//        }
//        return uid;
//    }
//    
//    public String getUserLoginById(int userId) {
//        String userLogin = "";
//        try {
//            String query = "SELECT user_login FROM colfusion_users WHERE user_id = " + userId;
//            Statement ps = connection.createStatement();
//            ResultSet result = ps.executeQuery(query);
//            while (result.next()) {
//                userLogin = result.getString("user_login");
//            }
//        } catch (SQLException e) {
//            e.printStackTrace();
//        }
//
//        return userLogin;
//    }
//
//    public boolean isBeingEditedByCurrentUser(int sid, String tableName, int userId) {
//        try {
//            String query = "SELECT endChangeTime FROM colfusion_table_change_log WHERE sid = " + sid
//                    + " AND tableName = '" + tableName + "' AND operatedUser = " + userId;
//            Statement ps = connection.createStatement();
//            ResultSet result = ps.executeQuery(query);
//            while (result.next()) {
//                if (result.getDate("endChangeTime") == null) return true;
//            }
//        } catch (SQLException e) {
//            e.printStackTrace();
//        }
//
//        return false;
//    }
//
//    public void releaseTableLock(int sid, String tableName) {
//        try {
//            SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");// set date format
//            String currentTime = format.format(new Date());// new Date() is get current system time
//
//            String query = "UPDATE colfusion_table_change_log SET endChangeTime = '" + currentTime + "' WHERE endChangeTime IS NULL AND sid = " + sid + " AND tableName = '" + tableName + "'";
//            Statement ps = connection.createStatement();
//            ps.executeUpdate(query);
//        } catch (SQLException e) {
//            e.printStackTrace();
//        }
//    }
//
//    public boolean isTimeOut(int sid, String tableName) {
//        SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");// set date format
//        String currentTime = format.format(new Date());// new Date() is to get current system time
//
//        String startTime = "";
//        try {
//            String query = "SELECT startChangeTime FROM colfusion_table_change_log WHERE endChangeTime is NULL and sid = "
//                    + sid + " AND tableName = '" + tableName + "'";
//            Statement ps = connection.createStatement();
//            ResultSet result = ps.executeQuery(query);
//            while (result.next()) {
//                startTime = result.getString("startChangeTime");
//            }
//        } catch (SQLException e) {
//            e.printStackTrace();
//        }
//        return isTimeOutHelper(currentTime, startTime);
//    }
//    
//    public boolean isTimeOutHelper(String currentTime, String startTime) {
//        if (!currentTime.split(" ")[0].equals(startTime.split(" ")[0])) {
//            return true;
//        }
//        startTime = startTime.substring(0, currentTime.length() - 1);
//        String[] arr1 = currentTime.split(" ")[1].split(":");
//        String[] arr2 = startTime.split(" ")[1].split(":");
//
//        int[] intArr1 = new int[arr1.length];
//        int[] intArr2 = new int[arr2.length];
//
//        for (int i = 0; i < arr1.length; i++)
//            intArr1[i] = Integer.valueOf(arr1[i]);
//        for (int j = 0; j < arr2.length; j++)
//            intArr2[j] = Integer.valueOf(arr2[j]);
//
//        if (intArr1[0] - intArr2[0] > 1) {
//            return true;
//        } else if (intArr1[0] - intArr2[0] == 1) {
//            if (intArr1[1] + 60 - intArr2[1] > 30) {
//                return true;
//            } else {
//                return false;
//            }
//        } else if (intArr1[0] - intArr2[0] == 0) {
//            if (intArr1[1] - intArr2[1] > 30) {
//                return true;
//            } else {
//                return false;
//            }
//        }
//        return false;
//    }
//
//    public void createEditLog(int sid, String tableName, int userId) {
//        SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");// set date format
//        String startEditTime = format.format(new Date());// new Date() is to get current system time
//
//        try {
//            String query = "INSERT INTO colfusion_table_change_log VALUES(" + sid + ", '" + tableName + "', '"
//                    + startEditTime + "', NULL, " + userId + ")";
//            Statement ps = connection.createStatement();
//            ps.executeUpdate(query);
//
//        } catch (SQLException e) {
//            e.printStackTrace();
//        }
//    }
//
//    public boolean isTableLocked(int sid, String tableName) {
//        try {
//            String query = "SELECT endChangeTime FROM colfusion_table_change_log WHERE sid = " + sid
//                    + " AND tableName = '" + tableName + "'";
//            Statement ps = connection.createStatement();
//            ResultSet result = ps.executeQuery(query);
//            while (result.next()) {
//                if (result.getDate("endChangeTime") == null) return true;
//            }
//        } catch (SQLException e) {
//            e.printStackTrace();
//        }
//
//        return false;
//    }
    
    public boolean isTempTableExist(final int sid, final String tableName) {
        String query = "SHOW TABLES LIKE 'temp_" + tableName + "'";
        Statement ps;
        try {
            ps = connection.createStatement();
            ResultSet result = ps.executeQuery(query);
            if (result.next()) {
                return true;
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return false;
    }

    public void removeTable(final int sid, final String tableName) {
        String query = "DROP TABLE " + tableName;
        Statement ps;
        try {
            ps = connection.createStatement();
            ps.execute(query);
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    public void backupOriginalTable(final int sid, final String tableName) {
        String query = "CREATE TABLE temp_" + tableName + " SELECT * FROM " + tableName;
        Statement ps;
        try {
            ps = connection.createStatement();
            ps.executeUpdate(query);
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    public boolean isMapExist(final int sid, final String tableName) {
        String selectQuery = "select * from colfusion_openrefine_project_map where sid = " + sid + " and tableName = '"
                + tableName + "'";
        Statement ps;
        try {
            ps = connection.createStatement();
            ResultSet rs = ps.executeQuery(selectQuery);

            while (rs.next()) {
                return true;
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
        
        return false;
    }

    public String getProjectId(final int sid, final String tableName) {
        String projectId = "";
        
        String selectQuery = "select projectId from colfusion_openrefine_project_map where sid = " + sid
                + " and tableName = '" + tableName + "'";
        Statement ps;
        try {
            ps = connection.createStatement();
            ResultSet rs = ps.executeQuery(selectQuery);
            while (rs.next()) {
                projectId = rs.getString(1);
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
        
        return projectId;
    }

    public void setProject(final int sid, final Project project) {
        // TODO: Have no idea how to rewrite this function
    }
    
    public void saveRelation(final long projectId, final int sid, final String tableName) {
        String insertQuery = String.format("insert into colfusion_openrefine_project_map values('%d', %d, '%s')",
                projectId, sid, tableName);
        Statement ps;
        try {
            ps = connection.createStatement();
            ps.executeUpdate(insertQuery);
        } catch (SQLException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }
    
    
    /*
     * Following functions are used only in "IsTableLockedCommand.java"
     */
    
    public boolean isTableBeingEdited(final int sid, final String tableName) {
        try {
            String query = "SELECT endChangeTime FROM colfusion_table_change_log WHERE sid = " + sid
                    + " AND tableName = '" + tableName + "'";
            Statement ps = connection.createStatement();
            ResultSet result = ps.executeQuery(query);
            while (result.next()) {
                if (result.getDate("endChangeTime") == null) {
                    return true;
                }
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }

        return false;
    }
    
    
    /*
     * Following functions are for "setProject()" method
     */
//    
//    public void setProjectColHelper(final int sid, final Project project) {
//        ArrayList<String> columnNames = new ArrayList<String>();
//        String colQuery = "select dname_chosen from colfusion_dnameinfo where sid = " + sid;
//        Statement ps;
//        try {
//            ps = connection.createStatement();
//            ResultSet rs = ps.executeQuery(colQuery);
//            
//            while (rs.next()) {
//                columnNames.add(rs.getString("dname_chosen"));
//            }
//        } catch (SQLException e) {
//            e.printStackTrace();
//        }
//        
//        setProjectCol(project, columnNames);
//    }
//    
//    public void setProjectRowHelper(final int sid, final Project project) {
//        String rowQuery1 = "select * from " + ;
//        Statement ps2 = conn1.createStatement();
//        Statement ps3 = conn1.createStatement();
//        ResultSet rs1 = ps2.executeQuery(rowQuery1);
//
//        ResultSet rsCount = ps3
//                .executeQuery("select count(*) from information_schema.columns where table_schema='colfusion_filetodb_"
//                        + sid + "' and table_name='" + tableName + "'");
//        int colCount = 0;
//        while (rsCount.next()) {
//            colCount = Integer.parseInt(rsCount.getString(1));
//        }
//
//        ArrayList<ArrayList<String>> rows = new ArrayList<ArrayList<String>>();
//
//        while (rs1.next()) {
//            int colIndex = 1;
//            ArrayList<String> temp = new ArrayList<String>();
//            while (colIndex <= colCount) {
//                temp.add(rs1.getString(colIndex));
//                colIndex++;
//            }
//            rows.add(temp);
//        }
//        setProjectRow(project, rows);
//    }
//    
//    public String getProjectTableName(final int sid, final Project project) {
//        String tableName = "";
//        String rowQuery = "select tableName from colfusion_columntableinfo where cid = (select cid from colfusion_dnameinfo where sid = "
//                + sid + " limit 1)";
//        Statement ps1;
//        try {
//            ps1 = connection.createStatement();
//            ResultSet newRs = ps1.executeQuery(rowQuery);
//            while (newRs.next()) {
//                tableName = newRs.getString("tableName");
//            }
//        } catch (SQLException e) {
//            e.printStackTrace();
//        }
//        return tableName;
//    }
//    
//    public static void setProjectCol(final Project project, final ArrayList<String> columnNames) {
//        for (int i = 0; i < columnNames.size(); i++) {
//            Column column = new Column(i, columnNames.get(i));
//            try {
//                project.columnModel.addColumn(i, column, true);
//            } catch (ModelException e) {
//                e.printStackTrace();
//            }
//        }
//    }
//
//    public static void setProjectRow(final Project project, final ArrayList<ArrayList<String>> rows) {
//        for (int j = 0; j < rows.size(); j++) {
//            Row row = new Row(rows.get(j).size());
//            for (int k = 0; k < rows.get(j).size(); k++) {
//                Cell cell = new Cell(rows.get(j).get(k), null);
//                row.setCell(k, cell);
//            }
//            project.rows.add(row);
//        }
//    }
    
}
