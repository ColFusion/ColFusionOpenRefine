
package com.google.refine.commands.colfusion;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.json.JSONException;
import org.json.JSONObject;

import com.google.refine.commands.Command;
import com.google.refine.util.ParsingUtilities;

public class IsTableLockedCommand extends Command {

    @Override
    public void doGet(HttpServletRequest request, HttpServletResponse response)
            throws IOException, ServletException {
        
        Properties parameters = ParsingUtilities.parseUrlParameters(request);

        int sid = Integer.valueOf(parameters.getProperty("sid"));
        String tableName = parameters.getProperty("tableName");
        int userId = Integer.valueOf(parameters.getProperty("userId"));

        boolean isTimeOut = false;
        boolean isTableBeingEditing = isTableBeingEditing(sid, tableName);
        boolean isEditingByCurrentUser = false;
        boolean isTableLocked = false;

        JSONObject result = new JSONObject();

        if (isTableBeingEditing) {
            isEditingByCurrentUser = isEditingByCurrentUser(sid, tableName, userId);
        }
        if (!isEditingByCurrentUser && (isTableBeingEditing && !isTimeOut)) {
            isTableLocked = true;
        }

        try {
            result.put("isTableLocked", isTableLocked);
            result.put("successful", true);
        } catch (JSONException e) {
            e.printStackTrace();
        }

        response.setCharacterEncoding("UTF-8");
        response.setHeader("Content-Type", "application/json");
        respond(response, result.toString());
    }

    public boolean isTableBeingEditing(int sid, String tableName) {
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

    public boolean isEditingByCurrentUser(int sid, String tableName, int userId) {
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
                    + " AND tableName = '" + tableName + "' AND operatedUser = " + userId;
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
}
