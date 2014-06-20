
package com.google.refine.commands.colfusion;

import java.io.IOException;
import java.sql.SQLException;
import java.util.Properties;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.json.JSONException;
import org.json.JSONObject;

import com.google.refine.commands.Command;
import com.google.refine.util.ParsingUtilities;

import edu.pitt.sis.exp.colfusion.dao.MetadataDbHandler;
import edu.pitt.sis.exp.colfusion.dao.TargetDatabaseHandlerFactory;

public class IsTableLockedCommand extends Command {

    @Override
    public void doGet(final HttpServletRequest request, final HttpServletResponse response)
            throws IOException, ServletException {

        Properties parameters = ParsingUtilities.parseUrlParameters(request);

        MetadataDbHandler metadataDbHandler = TargetDatabaseHandlerFactory.getMetadataDbHandler(); // colfusion
                                                                                                   // db

        int sid = Integer.valueOf(parameters.getProperty("sid"));
        String tableName = parameters.getProperty("tableName");
        int userId = Integer.valueOf(parameters.getProperty("userId"));
        try {
            boolean isTimeOut = false;
            boolean isTableBeingEditing = metadataDbHandler.isTableBeingEditing(sid, tableName);
            boolean isEditingByCurrentUser = false;
            boolean isTableLocked = false;

            JSONObject result = new JSONObject();

            String userLogin = "";

            if (isTableBeingEditing) {
                isEditingByCurrentUser = metadataDbHandler.isBeingEditedByCurrentUser(sid, tableName, userId);
                if (metadataDbHandler.isTimeOut(sid, tableName, 30)) {
                    isTimeOut = true;
                }
            }
            if (!isEditingByCurrentUser && (isTableBeingEditing && !isTimeOut)) {
                isTableLocked = true;
                userLogin = metadataDbHandler.getUserLoginById(metadataDbHandler.getOperatingUserId(sid, tableName));
            }

            result.put("userLogin", userLogin);
            result.put("isTableLocked", isTableLocked);
            result.put("successful", true);

            response.setCharacterEncoding("UTF-8");
            response.setHeader("Content-Type", "application/json");
            respond(response, result.toString());
        } catch (JSONException e) {
            e.printStackTrace();
        } catch (SQLException e) {
            e.printStackTrace();
        }

    }

    // public boolean isTimeOut(int sid, String tableName) {
    // String driver = "com.mysql.jdbc.Driver";
    // String dbURL = "jdbc:mysql://127.0.0.1:3306/colfusion"; // Connect
    // // to
    // // Server
    // // &
    // // DB连接服务器和数据库test
    // String userName = "root"; // UserName 用户名
    // String userPwd = ""; // Pwd 密码
    //
    // try {
    // Class.forName(driver);
    // } catch (ClassNotFoundException e) {
    // e.printStackTrace();
    // }
    // Connection conn;
    // SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");//
    // 设置日期格式
    // String currentTime = format.format(new Date());// new Date()为获取当前系统时间
    //
    // String startTime = "";
    // try {
    // conn = DriverManager.getConnection(dbURL, userName, userPwd);
    // String query =
    // "SELECT startChangeTime FROM colfusion_table_change_log WHERE endChangeTime is NULL and sid = "
    // + sid + " AND tableName = '" + tableName + "'";
    // Statement ps = conn.createStatement();
    // ResultSet result = ps.executeQuery(query);
    // while (result.next()) {
    // startTime = result.getString("startChangeTime");
    // }
    // } catch (SQLException e) {
    // e.printStackTrace();
    // }
    // return isTimeOutHelper(currentTime, startTime);
    // }
    //
    // public boolean isTimeOutHelper(String currentTime, String startTime) {
    // if (!currentTime.split(" ")[0].equals(startTime.split(" ")[0])) {
    // return true;
    // }
    // startTime = startTime.substring(0, currentTime.length() - 1);
    // String[] arr1 = currentTime.split(" ")[1].split(":");
    // String[] arr2 = startTime.split(" ")[1].split(":");
    //
    // int[] intArr1 = new int[arr1.length];
    // int[] intArr2 = new int[arr2.length];
    //
    // for (int i = 0; i < arr1.length; i++)
    // intArr1[i] = Integer.valueOf(arr1[i]);
    // for (int j = 0; j < arr2.length; j++)
    // intArr2[j] = Integer.valueOf(arr2[j]);
    //
    // if (intArr1[0] - intArr2[0] > 1) {
    // System.out.println("1st if");
    // return true;
    // } else if (intArr1[0] - intArr2[0] == 1) {
    // if (intArr1[1] + 60 - intArr2[1] > 30) {
    // System.out.println("2st if");
    // return true;
    // } else {
    // System.out.println("3st if");
    // return false;
    // }
    // } else if (intArr1[0] - intArr2[0] == 0) {
    // if (intArr1[1] - intArr2[1] > 30) {
    // System.out.println("2st if");
    // return true;
    // } else {
    // System.out.println("3st if");
    // return false;
    // }
    // }
    // System.out.println("4st if");
    // return false;
    // }
    //
    // public int getOperatingUser(int sid, String tableName) {
    // String driver = "com.mysql.jdbc.Driver";
    // String dbURL = "jdbc:mysql://127.0.0.1:3306/colfusion"; // Connect
    // // to
    // // Server
    // // &
    // // DB连接服务器和数据库test
    // String userName = "root"; // UserName 用户名
    // String userPwd = ""; // Pwd 密码
    //
    // int uid = 0;
    //
    // try {
    // Class.forName(driver);
    // } catch (ClassNotFoundException e) {
    // e.printStackTrace();
    // }
    // Connection conn;
    // try {
    // conn = DriverManager.getConnection(dbURL, userName, userPwd);
    // String query =
    // "SELECT operatedUser FROM colfusion_table_change_log WHERE endChangeTime IS NULL AND sid = "
    // + sid + " AND tableName = '" + tableName + "'";
    // Statement ps = conn.createStatement();
    // ResultSet result = ps.executeQuery(query);
    // while (result.next()) {
    // uid = Integer.valueOf(result.getString("operatedUser"));
    // }
    // } catch (SQLException e) {
    // e.printStackTrace();
    // }
    //
    // return uid;
    // }
    //
    // public String getUserLoginById(int userId) {
    // String driver = "com.mysql.jdbc.Driver";
    // String dbURL = "jdbc:mysql://127.0.0.1:3306/colfusion"; // Connect
    // // to
    // // Server
    // // &
    // // DB连接服务器和数据库test
    // String userName = "root"; // UserName 用户名
    // String userPwd = ""; // Pwd 密码
    //
    // String userLogin = "";
    //
    // try {
    // Class.forName(driver);
    // } catch (ClassNotFoundException e) {
    // e.printStackTrace();
    // }
    // Connection conn;
    // try {
    // conn = DriverManager.getConnection(dbURL, userName, userPwd);
    // String query = "SELECT user_login FROM colfusion_users WHERE user_id = "
    // + userId;
    // Statement ps = conn.createStatement();
    // ResultSet result = ps.executeQuery(query);
    // while (result.next()) {
    // userLogin = result.getString("user_login");
    // }
    // } catch (SQLException e) {
    // e.printStackTrace();
    // }
    //
    // return userLogin;
    // }
    //
    // public boolean isTableBeingEditing(int sid, String tableName) {
    // String driver = "com.mysql.jdbc.Driver";
    // String dbURL = "jdbc:mysql://127.0.0.1:3306/colfusion"; // Connect
    // // to
    // // Server
    // // &
    // // DB连接服务器和数据库test
    // String userName = "root"; // UserName 用户名
    // String userPwd = ""; // Pwd 密码
    //
    // try {
    // Class.forName(driver);
    // } catch (ClassNotFoundException e) {
    // e.printStackTrace();
    // }
    // Connection conn;
    // try {
    // conn = DriverManager.getConnection(dbURL, userName, userPwd);
    // String query =
    // "SELECT endChangeTime FROM colfusion_table_change_log WHERE sid = " + sid
    // + " AND tableName = '" + tableName + "'";
    // Statement ps = conn.createStatement();
    // ResultSet result = ps.executeQuery(query);
    // while (result.next()) {
    // if (result.getDate("endChangeTime") == null) return true;
    // }
    // } catch (SQLException e) {
    // e.printStackTrace();
    // }
    //
    // return false;
    // }
    //
    // public boolean isEditingByCurrentUser(int sid, String tableName, int
    // userId) {
    // String driver = "com.mysql.jdbc.Driver";
    // String dbURL = "jdbc:mysql://127.0.0.1:3306/colfusion"; // Connect
    // // to
    // // Server
    // // &
    // // DB连接服务器和数据库test
    // String userName = "root"; // UserName 用户名
    // String userPwd = ""; // Pwd 密码
    //
    // try {
    // Class.forName(driver);
    // } catch (ClassNotFoundException e) {
    // e.printStackTrace();
    // }
    // Connection conn;
    // try {
    // conn = DriverManager.getConnection(dbURL, userName, userPwd);
    // String query =
    // "SELECT endChangeTime FROM colfusion_table_change_log WHERE sid = " + sid
    // + " AND tableName = '" + tableName + "' AND operatedUser = " + userId;
    // Statement ps = conn.createStatement();
    // ResultSet result = ps.executeQuery(query);
    // while (result.next()) {
    // if (result.getDate("endChangeTime") == null) return true;
    // }
    // } catch (SQLException e) {
    // e.printStackTrace();
    // }
    //
    // return false;
    // }
}
