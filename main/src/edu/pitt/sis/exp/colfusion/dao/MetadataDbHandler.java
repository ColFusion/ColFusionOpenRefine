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

package edu.pitt.sis.exp.colfusion.dao;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class is used to read from colfusion main metadata database
 * 
 * @author xxl
 * 
 */
public class MetadataDbHandler {

    private final DatabaseHandler dbHandler;

    final static Logger logger = LoggerFactory.getLogger(MetadataDbHandler.class.getName());

    public MetadataDbHandler(final DatabaseHandler dbHandler) {
        this.dbHandler = dbHandler;
    }

    public DatabaseConnectionInfo getTargetDbConnectionInfo(final int sid)
            throws SQLException {
        logger.info(String.format("Getting target database connectio info for sid %d", sid));

        try (Connection connection = dbHandler.getConnection()) {

            String sql = "SELECT * FROM colfusion_sourceinfo_DB WHERE sid = ?";

            try (PreparedStatement statement = connection.prepareStatement(sql)) {
                statement.setInt(1, sid);

                ResultSet rs = statement.executeQuery();
                while (rs.next()) {
                    return new DatabaseConnectionInfo(rs.getString("server_address"), rs.getInt("port"),
                            rs.getString("user_name"), rs.getString("password"), rs.getString("source_database"));
                }

                String message = String.format("Select * from sourceinfo_db returned 0 rows for sid %d", sid);

                logger.error(message);
                throw new RuntimeException(message);
            } catch (SQLException e) {
                logger.error(String.format("Select * from sourceinfo_db FAILED for sid %d", sid), e);

                throw e;
            }
        } catch (SQLException e) {
            logger.info(String.format("FAILED to getting target database connectio info for sid %d. "
                    + "Something was wrong when opening connection", sid));
            throw e;
        }
    }

    /**
     * 
     * @param sid
     * @param tableName
     * @return
     * @throws SQLException
     */
    public int getOperatingUserId(final int sid, final String tableName)
            throws SQLException {
        logger.info(String.format("Getting operating user id for sid %d and table name %s", sid, tableName));

        try (Connection connection = dbHandler.getConnection()) {

            String sql = "SELECT operatedUser FROM colfusion_table_change_log WHERE endChangeTime IS NULL AND sid = ? AND tableName = ?";

            try (PreparedStatement statement = connection.prepareStatement(sql)) {
                statement.setInt(1, sid);
                statement.setString(2, tableName);

                ResultSet rs = statement.executeQuery();
                while (rs.next()) {
                    return rs.getInt("operatedUser");
                }

                String message = String.format(
                        "Getting operating user id for sid %d and table name %s returned 0 rows", sid, tableName);

                logger.error(message);
                throw new RuntimeException(message);
            } catch (SQLException e) {
                logger.error(
                        String.format("Getting operating user id for sid %d and table name %s FAILED", sid, tableName),
                        e);

                throw e;
            }
        } catch (SQLException e) {
            logger.info(String.format("FAILED to getting operating user id for sid %d and table name %s ", sid,
                    tableName));
            throw e;
        }
    }

    public String getUserLoginById(final int userId)
            throws SQLException {

        logger.info(String.format("Getting user login for userId %d", userId));
        String userLogin = "";
        try (Connection connection = dbHandler.getConnection()) {

            String sql = "SELECT user_login FROM colfusion_users WHERE user_id = ?";

            try (PreparedStatement statement = connection.prepareStatement(sql)) {
                statement.setInt(1, userId);

                ResultSet rs = statement.executeQuery();
                while (rs.next()) {
                    userLogin = rs.getString("user_login");
                }

                return userLogin;

            } catch (SQLException e) {
                logger.error(String.format("Getting user login for userId %d FAILED", userId), e);

                throw e;
            }
        } catch (SQLException e) {
            logger.info(String.format("FAILED to getting user login for userId %d ", userId));
            throw e;
        }

    }

    public boolean isBeingEditedByCurrentUser(final int sid, final String tableName, final int userId)
            throws SQLException {

        logger.info(String.format("Getting if current table is being edited by user %d", userId));
        try (Connection connection = dbHandler.getConnection()) {

            String sql = "SELECT endChangeTime FROM colfusion_table_change_log WHERE sid = ? AND tableName = ? AND operatedUser = ?";

            try (PreparedStatement statement = connection.prepareStatement(sql)) {
                statement.setInt(1, sid);
                statement.setString(2, tableName);
                statement.setInt(3, userId);

                ResultSet rs = statement.executeQuery();
                while (rs.next()) {
                    if (rs.getDate("endChangeTime") == null) {
                        return true;
                    }
                }

                return false;

            } catch (SQLException e) {
                logger.error(String.format("Getting if current table is being edited by user %d FAILED", userId), e);

                throw e;
            }
        } catch (SQLException e) {
            logger.info(String.format("FAILED to getting if current table is being edited by user %d ", userId));
            throw e;
        }
    }

    public void releaseTableLock(final int sid, final String tableName)
            throws SQLException {
        logger.info(String.format("Releasing table for sid %d and table %s", sid, tableName));

        SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        String currentTime = format.format(new Date());

        try (Connection connection = dbHandler.getConnection()) {

            String sql = String
                    .format("UPDATE colfusion_table_change_log SET endChangeTime = '%s' WHERE endChangeTime IS NULL AND sid = %d  AND tableName = '%s'",
                            currentTime, sid, tableName);

            try (Statement statement = connection.createStatement()) {

                statement.executeUpdate(sql);

            } catch (SQLException e) {
                logger.error(String.format("Releasing table for sid %d and table %s FAILED", sid, tableName), e);

                throw e;
            }
        } catch (SQLException e) {
            logger.info(String.format("FAILED to releasing table for sid %d and table %s ", sid, tableName));
            throw e;
        }
    }

    public boolean isTimeOut(final int sid, final String tableName, final int allowedTime)
            throws SQLException {
        logger.info(String.format("Getting if it is timeout for sid %d and table %s", sid, tableName));

        SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        String currentTime = format.format(new Date());

        String startTime = "";

        try (Connection connection = dbHandler.getConnection()) {

            String sql = "SELECT startChangeTime FROM colfusion_table_change_log WHERE endChangeTime is NULL and sid = ? AND tableName = ?";

            try (PreparedStatement statement = connection.prepareStatement(sql)) {

                statement.setInt(1, sid);
                statement.setString(2, tableName);

                ResultSet rs = statement.executeQuery();
                while (rs.next()) {
                    startTime = rs.getString("startChangeTime");
                }
                if(startTime == "") {
                    return false;
                } else {
                    return isTimeOutHelper(currentTime, startTime, allowedTime);
                }

            } catch (SQLException e) {
                logger.error(String.format("Getting if it is timeout for sid %d and table %s FAILED", sid, tableName),
                        e);

                throw e;
            }
        } catch (SQLException e) {
            logger.info(String.format("FAILED to getting if it is timeout for sid %d and table %s ", sid, tableName));
            throw e;
        }
    }
    
    public boolean isTimeOutForCurrentUser(final int sid, final String tableName, final int colfusionUserId, final int allowedTime)
            throws SQLException {
        logger.info(String.format("Getting if it is timeout for sid %d and table %s", sid, tableName));

        SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        String currentTime = format.format(new Date());

        String startTime = "";

        try (Connection connection = dbHandler.getConnection()) {

            String sql = "SELECT startChangeTime FROM colfusion_table_change_log WHERE endChangeTime is NULL and sid = ? AND tableName = ? AND operatedUser = ?";

            try (PreparedStatement statement = connection.prepareStatement(sql)) {

                statement.setInt(1, sid);
                statement.setString(2, tableName);
                statement.setInt(3, colfusionUserId);

                ResultSet rs = statement.executeQuery();
                while (rs.next()) {
                    startTime = rs.getString("startChangeTime");
                    return isTimeOutHelper(currentTime, startTime, allowedTime);
                }
                return true;

            } catch (SQLException e) {
                logger.error(String.format("Getting if it is timeout for sid %d and table %s FAILED", sid, tableName),
                        e);

                throw e;
            }
        } catch (SQLException e) {
            logger.info(String.format("FAILED to getting if it is timeout for sid %d and table %s ", sid, tableName));
            throw e;
        }
    }
    

    public boolean isTimeOutHelper(final String currentTime, String startTime, final int allowedTime) {
        if (!currentTime.split(" ")[0].equals(startTime.split(" ")[0])) {
            return true;
        }
        startTime = startTime.substring(0, currentTime.length() - 1);
        String[] arr1 = currentTime.split(" ")[1].split(":");
        String[] arr2 = startTime.split(" ")[1].split(":");

        int[] intArr1 = new int[arr1.length];
        int[] intArr2 = new int[arr2.length];

        for (int i = 0; i < arr1.length; i++) {
            intArr1[i] = Integer.valueOf(arr1[i]);
        }
        for (int j = 0; j < arr2.length; j++) {
            intArr2[j] = Integer.valueOf(arr2[j]);
        }

        if (intArr1[0] - intArr2[0] > 1) {
            return true;
        } else if (intArr1[0] - intArr2[0] == 1) {
            if (intArr1[1] + 60 - intArr2[1] > allowedTime) {
                return true;
            } else {
                return false;
            }
        } else if (intArr1[0] - intArr2[0] == 0) {
            if (intArr1[1] - intArr2[1] > allowedTime) {
                return true;
            } else {
                return false;
            }
        }
        return false;
    }

    public void createEditLog(final int sid, final String tableName, final int userId)
            throws SQLException {
        logger.info(String.format("Creating edit log for sid %d, table %s and user %d", sid, tableName, userId));

        SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        String startEditTime = format.format(new Date());

        try (Connection connection = dbHandler.getConnection()) {

            String sql = String.format("INSERT INTO colfusion_table_change_log VALUES(%d, '%s', '%s', NULL, %d)", sid,
                    tableName, startEditTime, userId);

            try (Statement statement = connection.createStatement()) {

                statement.executeUpdate(sql);

            } catch (SQLException e) {
                logger.error(String.format("Creating edit log for sid %d, table %s and user %d FAILED", sid, tableName,
                        userId), e);

                throw e;
            }
        } catch (SQLException e) {
            logger.info(String.format("FAILED to creating edit log for sid %d, table %s and user %d", sid, tableName,
                    userId));
            throw e;
        }
    }

    public boolean isTableLocked(final int sid, final String tableName)
            throws SQLException {

        logger.info(String.format("Getting if table locked for sid %d, table %s", sid, tableName));
        try (Connection connection = dbHandler.getConnection()) {

            String sql = "SELECT endChangeTime FROM colfusion_table_change_log WHERE sid = ?  AND tableName = ?";

            try (PreparedStatement statement = connection.prepareStatement(sql)) {

                statement.setInt(1, sid);
                statement.setString(2, tableName);

                ResultSet rs = statement.executeQuery();
                while (rs.next()) {
                    if (rs.getDate("endChangeTime") == null) {
                        return true;
                    }
                }
                return false;

            } catch (SQLException e) {
                logger.error(String.format("Getting if table locked for sid %d, table %s FAILED", sid, tableName), e);

                throw e;
            }
        } catch (SQLException e) {
            logger.info(String.format("FAILED to getting if table locked for sid %d, table %s", sid, tableName));
            throw e;
        }
    }

    public boolean isMapExist(final int sid, final String tableName)
            throws SQLException {
        logger.info(String.format("Getting if map exists for sid %d, table %s", sid, tableName));
        try (Connection connection = dbHandler.getConnection()) {

            String sql = "select * from colfusion_openrefine_project_map where sid = ?  and tableName = ?";

            try (PreparedStatement statement = connection.prepareStatement(sql)) {

                statement.setInt(1, sid);
                statement.setString(2, tableName);

                ResultSet rs = statement.executeQuery();

                while (rs.next()) {
                    return true;
                }
                return false;

            } catch (SQLException e) {
                logger.error(String.format("Getting if map exists for sid %d, table %s FAILED", sid, tableName), e);

                throw e;
            }
        } catch (SQLException e) {
            logger.info(String.format("FAILED to getting if map exists for sid %d, table %s", sid, tableName));
            throw e;
        }
    }

    public String getProjectId(final int sid, final String tableName)
            throws SQLException {
        logger.info(String.format("Getting projectId for sid %d, table %s", sid, tableName));
        String projectId = "";
        try (Connection connection = dbHandler.getConnection()) {

            String sql = "select projectId from colfusion_openrefine_project_map where sid = ? and tableName = ?";

            try (PreparedStatement statement = connection.prepareStatement(sql)) {

                statement.setInt(1, sid);
                statement.setString(2, tableName);

                ResultSet rs = statement.executeQuery();
                while (rs.next()) {
                    projectId = rs.getString(1);
                }
                return projectId;

            } catch (SQLException e) {
                logger.error(String.format("Getting projectId for sid %d, table %s FAILED", sid, tableName), e);

                throw e;
            }
        } catch (SQLException e) {
            logger.info(String.format("FAILED to getting projectId for sid %d, table %s", sid, tableName));
            throw e;
        }
    }

    public boolean isTableBeingEditing(final int sid, final String tableName)
            throws SQLException {
        logger.info(String.format("Getting if table is being edited for sid %d, table %s", sid, tableName));
        try (Connection connection = dbHandler.getConnection()) {

            String sql = "SELECT endChangeTime FROM colfusion_table_change_log WHERE sid = ? AND tableName = ?";

            try (PreparedStatement statement = connection.prepareStatement(sql)) {

                statement.setInt(1, sid);
                statement.setString(2, tableName);

                ResultSet rs = statement.executeQuery();
                while (rs.next()) {
                    if (rs.getDate("endChangeTime") == null) {
                        return true;
                    }
                }
                return false;

            } catch (SQLException e) {
                logger.error(
                        String.format("Getting if table is being edited for sid %d, table %s FAILED", sid, tableName),
                        e);

                throw e;
            }
        } catch (SQLException e) {
            logger.info(String
                    .format("FAILED to getting if table is being edited for sid %d, table %s", sid, tableName));
            throw e;
        }
    }

    public void saveRelation(final long projectId, final int sid, final String tableName)
            throws SQLException {
        logger.info(String.format("Saving relation for project %d, sid %d and tablename %s", projectId, sid, tableName));

        try (Connection connection = dbHandler.getConnection()) {

            String sql = String.format("insert into colfusion_openrefine_project_map values('%d', %d, '%s')",
                    projectId, sid, tableName);

            try (Statement statement = connection.createStatement()) {

                statement.executeUpdate(sql);

            } catch (SQLException e) {
                logger.error(
                        String.format("Saving relation for project %d, sid %d and tablename %s FAILED", sid, tableName),
                        e);

                throw e;
            }
        } catch (SQLException e) {
            logger.info(String.format("FAILED to saving relation for project %d, sid %d and tablename %s", sid,
                    tableName));
            throw e;
        }
    }

    public ArrayList<String> getColumnNames(final int sid)
            throws SQLException {
        logger.info(String.format("Getting column names for sid %d", sid));
        ArrayList<String> columnNames = new ArrayList<String>();
        try (Connection connection = dbHandler.getConnection()) {

            String sql = "select dname_chosen from colfusion_dnameinfo where sid = ?";

            try (PreparedStatement statement = connection.prepareStatement(sql)) {
                statement.setInt(1, sid);

                ResultSet rs = statement.executeQuery();

                while (rs.next()) {
                    columnNames.add(rs.getString("dname_chosen"));
                }
                return columnNames;
            } catch (SQLException e) {
                logger.error(String.format("Getting column names for sid %d FAILED", sid), e);

                throw e;
            }
        } catch (SQLException e) {
            logger.info(String.format("FAILED to Getting column names for sid %d", sid));
            throw e;
        }
    }

    public String getTableName(final int sid)
            throws SQLException {
        logger.info(String.format("Getting tableName for sid %d", sid));

        try (Connection connection = dbHandler.getConnection()) {

            String sql = "select tableName from colfusion_columntableinfo where cid = (select cid from colfusion_dnameinfo where sid = ?  limit 1)";

            try (PreparedStatement statement = connection.prepareStatement(sql)) {
                statement.setInt(1, sid);

                ResultSet rs = statement.executeQuery();
                String tableName = "";
                while (rs.next()) {
                    tableName = rs.getString("tableName");
                }
                return tableName;
            } catch (SQLException e) {
                logger.error(String.format("Getting tableName for sid %d FAILED", sid), e);

                throw e;
            }
        } catch (SQLException e) {
            logger.info(String.format("FAILED to getting tableName for sid %d", sid));
            throw e;
        }
    }

    public boolean isInCurrentUserSession(final int sid, final String tableName, final int colfusionUserId)
            throws SQLException {
        logger.info(String.format("Getting if still in the session for sid %d, tableName %s and user %d", sid, tableName, colfusionUserId));
        
        try (Connection connection = dbHandler.getConnection()) {

            String sql = "SELECT * FROM colfusion_table_change_log WHERE endChangeTime is NULL and sid = ? and tableName = ? and operatedUser = ?";

            try (PreparedStatement statement = connection.prepareStatement(sql)) {
                statement.setInt(1, sid);
                statement.setString(2, tableName);
                statement.setInt(3, colfusionUserId);

                ResultSet rs = statement.executeQuery();


                while (rs.next()) {
                    return true;
                }
                return false;
            } catch (SQLException e) {
                logger.error(String.format("Getting if still in the session for sid %d, tableName %s and user %d FAILED", sid, tableName, colfusionUserId), e);

                throw e;
            }
        } catch (SQLException e) {
            logger.info(String.format("FAILED to getting if still in the session for sid %d, tableName %s and user %d", sid, tableName, colfusionUserId));
            throw e;
        }
    }
    
    public int getSid(final String projectId) throws SQLException {
        logger.info(String.format("Getting sid for project %s", projectId));
        
        try (Connection connection = dbHandler.getConnection()) {

            
            String sql = "select sid from colfusion_openrefine_project_map where projectId = ?";
            
            String sid = "";
            
            try (PreparedStatement statement = connection.prepareStatement(sql)) {
                statement.setString(1, projectId);

                ResultSet rs = statement.executeQuery();


                while (rs.next()) {
                    sid = rs.getString(1);
            }
                return Integer.valueOf(sid);
            } catch (SQLException e) {
                logger.error(String.format("Getting sid for project %s FAILED", projectId), e);

                throw e;
            }
        } catch (SQLException e) {
            logger.info(String.format("FAILED to getting sid for project %s", projectId));
            throw e;
        }
    }
    
    public String getTableNameByProjectId(final String projectId) throws SQLException {
        logger.info(String.format("Getting tableName for project %s", projectId));

        try (Connection connection = dbHandler.getConnection()) {

            String sql = "select tableName from colfusion_openrefine_project_map where projectId = ?";

            try (PreparedStatement statement = connection.prepareStatement(sql)) {
                statement.setString(1, projectId);

                ResultSet rs = statement.executeQuery();
                String tableName = "";
                while (rs.next()) {
                    tableName = rs.getString("tableName");
                }
                return tableName;
            } catch (SQLException e) {
                logger.error(String.format("Getting tableName for project %s FAILED", projectId), e);

                throw e;
            }
        } catch (SQLException e) {
            logger.info(String.format("FAILED to getting tableName for project %s", projectId));
            throw e;
        }
    }

}
