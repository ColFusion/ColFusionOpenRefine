
package com.google.refine.commands.colfusion;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.Properties;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.json.JSONException;
import org.json.JSONObject;

import com.google.refine.ProjectManager;
import com.google.refine.ProjectMetadata;
import com.google.refine.commands.Command;
import com.google.refine.io.FileProjectManager;
import com.google.refine.model.Cell;
import com.google.refine.model.Column;
import com.google.refine.model.ModelException;
import com.google.refine.model.Project;
import com.google.refine.model.Row;
import com.google.refine.util.ParsingUtilities;

/**
 * @author Xiaolong Xu
 * 
 */
public class CreateProjectFromColfusionStoryCommand extends Command {

    @Override
    public void doGet(HttpServletRequest request, HttpServletResponse response)
            throws ServletException, IOException {

        Properties parameters = ParsingUtilities.parseUrlParameters(request);
        /*
         * Get "sid" and "tableName" from "request"
         */
        int sid = Integer.valueOf(parameters.getProperty("sid"));
        String tableName = parameters.getProperty("tableName");

        JSONObject result = new JSONObject();

        boolean isTimeOut = false;
        boolean isTableLocked = isTableLocked(sid, tableName);

        String msg = "";

        if (isTableLocked) {
            msg += "Table is locked! ";
            if (isTimeOut(sid, tableName)) {
                msg += "Someone was editing this table, but time's out now, so release this table for you! ";
                isTimeOut = true;
                releaseTableLock(sid, tableName);
            }
        }
        // if (true) {
        if (isTableLocked && !isTimeOut) {
            msg += "Someone is editing this table!";
        } else {
            // TODO: CurrentUser needs to be a parameter but not hard code which
            // is from JavaScript
            String currentUser = "xxl2";
            createEditLog(sid, tableName, currentUser);

            ProjectManager.singleton.setBusy(true);
            try {
                /*
                 * If there is no temp table, then backup original table to a
                 * temp_ table
                 */
                if (tempTbExist(sid, tableName)) {
                    rmTb(sid, "temp_" + tableName);
                    backupOriTb(sid, tableName);
                } else {
                    backupOriTb(sid, tableName);
                }
                /*
                 * Create a new project for the table in the database
                 */
                String projectLink = "";

                if (isMapExist(sid, tableName)) {
                    projectLink = getProjectId(sid, tableName);
                } else {
                    Project project = new Project();
                    ProjectMetadata ppm = new ProjectMetadata();

                    ppm.setName(tableName);
                    ppm.setEncoding("UTF-8");
                    setProject(sid, project);

                    File dir = new File("C:\\Users\\xxl\\AppData\\Roaming\\OpenRefine");

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
                     * reason is because without the "save()", some stuff will
                     * exist in cache so that the new created project cannot be
                     * stored correctly
                     */
                    project.dispose();
                    FileProjectManager.singleton.save(true);
                    /*
                     * ********************************************
                     */

                    saveRelation(project.id, sid, tableName);

                    projectLink = project.id + "";
                }

                String url = "http://127.0.0.1:3333/project?project=" + projectLink;
                URI uri = null;
                try {
                    uri = new java.net.URI(url);
                } catch (Exception e) {
                    System.out.println("Failed to load webpage!");
                }
                try {
                    java.awt.Desktop.getDesktop().browse(uri);
                } catch (Exception e) {
                    System.out.println("Failed to open browser");
                }

            } catch (Exception e) {
                respondWithErrorPage(request, response, "Failed to import file", e);
            } finally {
                ProjectManager.singleton.setBusy(false);
            }
        }

        try {
            // result.put("testMsg", testMsg);
            result.put("isEditing", isTableLocked);
            result.put("msg", msg);
            result.put("successful", true);
        } catch (JSONException e) {
            e.printStackTrace();
        }

        response.setCharacterEncoding("UTF-8");
        response.setHeader("Content-Type", "application/json");
        respond(response, result.toString());
    }

    public void releaseTableLock(int sid, String tableName) {
        String driver = "com.mysql.jdbc.Driver";
        String dbURL = "jdbc:mysql://127.0.0.1:3306/colfusion"; // Connect
                                                                // to
                                                                // Server
                                                                // &
                                                                // DB连接服务器和数据库test
        String userName = "root"; // UserName 用户名
        String userPwd = ""; // Pwd 密码

        try {
            Class.forName(driver);
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }
        Connection conn;
        try {
            conn = DriverManager.getConnection(dbURL, userName, userPwd);

            SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");// 设置日期格式
            String currentTime = format.format(new Date());// new
                                                           // Date()为获取当前系统时间

            String query = "UPDATE colfusion_table_change_log SET endChangeTime = '" + currentTime + "' WHERE endChangeTime IS NULL AND sid = " + sid + " AND tableName = '" + tableName + "'";
            Statement ps = conn.createStatement();
            ps.executeUpdate(query);
        } catch (SQLException e) {
            e.printStackTrace();
        }

    }

    public boolean isTimeOut(int sid, String tableName) {
        String driver = "com.mysql.jdbc.Driver";
        String dbURL = "jdbc:mysql://127.0.0.1:3306/colfusion"; // Connect
                                                                // to
                                                                // Server
                                                                // &
                                                                // DB连接服务器和数据库test
        String userName = "root"; // UserName 用户名
        String userPwd = ""; // Pwd 密码

        try {
            Class.forName(driver);
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }
        Connection conn;
        SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");// 设置日期格式
        String currentTime = format.format(new Date());// new Date()为获取当前系统时间

        String startTime = "";
        try {
            conn = DriverManager.getConnection(dbURL, userName, userPwd);
            String query = "SELECT startChangeTime FROM colfusion_table_change_log WHERE endChangeTime is NULL and sid = "
                    + sid + " AND tableName = '" + tableName + "'";
            Statement ps = conn.createStatement();
            ResultSet result = ps.executeQuery(query);
            while (result.next()) {
                startTime = result.getString("startChangeTime");
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return isTimeOutHelper(currentTime, startTime);
    }

    public boolean isTimeOutHelper(String currentTime, String startTime) {
        if (!currentTime.split(" ")[0].equals(startTime.split(" ")[0])) {
            return true;
        }
        startTime = startTime.substring(0, currentTime.length() - 1);
        String[] arr1 = currentTime.split(" ")[1].split(":");
        String[] arr2 = startTime.split(" ")[1].split(":");

        int[] intArr1 = new int[arr1.length];
        int[] intArr2 = new int[arr2.length];

        for (int i = 0; i < arr1.length; i++)
            intArr1[i] = Integer.valueOf(arr1[i]);
        for (int j = 0; j < arr2.length; j++)
            intArr2[j] = Integer.valueOf(arr2[j]);

        if (intArr1[0] - intArr2[0] > 1) {
            System.out.println("1st if");
            return true;
        } else if (intArr1[0] - intArr2[0] == 1) {
            if (intArr1[1] + 60 - intArr2[1] > 30) {
                System.out.println("2st if");
                return true;
            } else {
                System.out.println("3st if");
                return false;
            }
        } else if (intArr1[0] - intArr2[0] == 0) {
            if (intArr1[1] - intArr2[1] > 30) {
                System.out.println("2st if");
                return true;
            } else {
                System.out.println("3st if");
                return false;
            }
        }
        System.out.println("4st if");
        return false;
    }

    public void createEditLog(int sid, String tableName, String currentUser) {
        String driver = "com.mysql.jdbc.Driver";
        String dbURL = "jdbc:mysql://127.0.0.1:3306/colfusion"; // Connect
                                                                // to
                                                                // Server
                                                                // &
                                                                // DB连接服务器和数据库test
        String userName = "root"; // UserName 用户名
        String userPwd = ""; // Pwd 密码

        try {
            Class.forName(driver);
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }
        Connection conn;

        SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");// 设置日期格式
        String startEditTime = format.format(new Date());// new Date()为获取当前系统时间

        try {
            conn = DriverManager.getConnection(dbURL, userName, userPwd);
            String query = "INSERT INTO colfusion_table_change_log VALUES(" + sid + ", '" + tableName + "', '"
                    + startEditTime + "', NULL, '" + currentUser + "')";
            Statement ps = conn.createStatement();
            ps.executeUpdate(query);

        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    public boolean isTableLocked(int sid, String tableName) {
        String driver = "com.mysql.jdbc.Driver";
        String dbURL = "jdbc:mysql://127.0.0.1:3306/colfusion"; // Connect
                                                                // to
                                                                // Server
                                                                // &
                                                                // DB连接服务器和数据库test
        String userName = "root"; // UserName 用户名
        String userPwd = ""; // Pwd 密码

        try {
            Class.forName(driver);
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }
        Connection conn;
        try {
            conn = DriverManager.getConnection(dbURL, userName, userPwd);
            String query = "SELECT endChangeTime FROM colfusion_table_change_log WHERE sid = " + sid
                    + " AND tableName = '" + tableName + "'";
            Statement ps = conn.createStatement();
            ResultSet result = ps.executeQuery(query);
            while (result.next()) {
                if (result.getDate("endChangeTime") == null) return true;
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }

        return false;
    }

    public boolean tempTbExist(int sid, String tableName)
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
            e.printStackTrace();
        }
        Connection conn = DriverManager.getConnection(dbURL, userName, userPwd);

        String query = "SHOW TABLES LIKE 'temp_" + tableName + "'";
        Statement ps = conn.createStatement();
        ResultSet result = ps.executeQuery(query);
        if (result.next())
            return true;
        else {
            return false;
        }
    }

    public void rmTb(int sid, String tableName)
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
            e.printStackTrace();
        }
        Connection conn = DriverManager.getConnection(dbURL, userName, userPwd);

        String query = "DROP TABLE " + tableName;
        Statement ps = conn.createStatement();
        ps.execute(query);
    }

    public void backupOriTb(int sid, String tableName)
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
            e.printStackTrace();
        }
        Connection conn = DriverManager.getConnection(dbURL, userName, userPwd);

        String query = "CREATE TABLE temp_" + tableName + " SELECT * FROM " + tableName;
        Statement ps = conn.createStatement();
        ps.executeUpdate(query);
    }

    public boolean isMapExist(int sid, String tableName)
            throws SQLException {
        String driver = "com.mysql.jdbc.Driver";
        String dbURL = "jdbc:mysql://127.0.0.1:3306/colfusion";
        String userName = "root"; // UserName 用户名
        String userPwd = ""; // Pwd 密码

        Connection conn = null;
        try {
            Class.forName(driver);
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }
        conn = DriverManager.getConnection(dbURL, userName, userPwd);

        String selectQuery = "select * from colfusion_openrefine_project_map where sid = " + sid + " and tableName = '"
                + tableName + "'";
        Statement ps = conn.createStatement();
        ResultSet rs = ps.executeQuery(selectQuery);

        while (rs.next()) {
            return true;
        }
        return false;
    }

    public String getProjectId(int sid, String tableName)
            throws SQLException {
        String driver = "com.mysql.jdbc.Driver";
        String dbURL = "jdbc:mysql://127.0.0.1:3306/colfusion";
        String userName = "root"; // UserName 用户名
        String userPwd = ""; // Pwd 密码

        Connection conn = null;
        try {
            Class.forName(driver);
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }
        conn = DriverManager.getConnection(dbURL, userName, userPwd);

        String selectQuery = "select projectId from colfusion_openrefine_project_map where sid = " + sid
                + " and tableName = '" + tableName + "'";
        Statement ps = conn.createStatement();
        ResultSet rs = ps.executeQuery(selectQuery);

        String projectId = "";
        while (rs.next()) {
            projectId = rs.getString(1);
        }
        return projectId;
    }

    public static void setProject(int sid, Project project)
            throws SQLException {
        String driver = "com.mysql.jdbc.Driver";
        String dbURL = "jdbc:mysql://127.0.0.1:3306/colfusion";
        String userName = "root"; // UserName 用户名
        String userPwd = ""; // Pwd 密码

        Connection conn = null;
        try {
            Class.forName(driver);
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }
        conn = DriverManager.getConnection(dbURL, userName, userPwd);

        // Get and set column names
        String colQuery = "select dname_chosen from colfusion_dnameinfo where sid = " + sid;
        Statement ps = conn.createStatement();
        ResultSet rs = ps.executeQuery(colQuery);
        ArrayList<String> columnNames = new ArrayList<String>();
        while (rs.next()) {
            columnNames.add(rs.getString("dname_chosen"));
        }
        setProjectCol(project, columnNames);

        // Get and set rows
        String rowQuery = "select tableName from colfusion_columntableinfo where cid = (select cid from colfusion_dnameinfo where sid = "
                + sid + " limit 1)";
        Statement ps1 = conn.createStatement();

        ResultSet newRs = ps1.executeQuery(rowQuery);
        String tableName = "";
        while (newRs.next()) {
            tableName = newRs.getString("tableName");
        }
        System.out.println("***************setProject*************************");
        System.out.println(tableName);
        System.out.println("***************setProject*************************");
        Connection conn1 = null;
        String dbURL1 = "jdbc:mysql://127.0.0.1:3306/colfusion_filetodb_" + sid;
        conn1 = DriverManager.getConnection(dbURL1, userName, userPwd);
        String rowQuery1 = "select * from " + tableName;
        Statement ps2 = conn1.createStatement();
        Statement ps3 = conn1.createStatement();
        ResultSet rs1 = ps2.executeQuery(rowQuery1);

        ResultSet rsCount = ps3
                .executeQuery("select count(*) from information_schema.columns where table_schema='colfusion_filetodb_"
                        + sid + "' and table_name='" + tableName + "'");
        int colCount = 0;
        while (rsCount.next()) {
            colCount = Integer.parseInt(rsCount.getString(1));
        }

        ArrayList<ArrayList<String>> rows = new ArrayList<ArrayList<String>>();

        while (rs1.next()) {
            int colIndex = 1;
            ArrayList<String> temp = new ArrayList<String>();
            while (colIndex <= colCount) {
                temp.add(rs1.getString(colIndex));
                colIndex++;
            }
            rows.add(temp);
        }
        setProjectRow(project, rows);
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
                row.setCell(k, cell);
            }
            project.rows.add(row);
        }
    }

    public static void saveRelation(long projectId, int sid, String tableName)
            throws SQLException {
        String driver = "com.mysql.jdbc.Driver";
        String dbURL = "jdbc:mysql://127.0.0.1:3306/colfusion";
        String userName = "root"; // UserName 用户名
        String userPwd = ""; // Pwd 密码

        Connection conn = null;
        try {
            Class.forName(driver);
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }
        conn = DriverManager.getConnection(dbURL, userName, userPwd);

        String insertQuery = String.format("insert into colfusion_openrefine_project_map values('%d', %d, '%s')",
                projectId, sid, tableName);
        Statement ps = conn.createStatement();
        ps.executeUpdate(insertQuery);
    }
}
