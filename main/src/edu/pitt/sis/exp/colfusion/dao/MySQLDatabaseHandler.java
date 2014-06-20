
package edu.pitt.sis.exp.colfusion.dao;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class is used to read actual data values from target databases;
 * 
 * @author xxl
 * 
 */
public class MySQLDatabaseHandler extends DatabaseHandler {

    final static Logger logger = LoggerFactory.getLogger(MySQLDatabaseHandler.class.getName());

    public MySQLDatabaseHandler(final DatabaseConnectionInfo databaseConnectionInfo) throws ClassNotFoundException {
        super(databaseConnectionInfo);// TODO Auto-generated constructor stub

        Class.forName("com.mysql.jdbc.Driver");
    }

    @Override
    protected Connection getConnection()
            throws SQLException {
        String connectionString = getConnectionString();

        try {
            return DriverManager.getConnection(connectionString, databaseConnectionInfo.getUser(),
                    databaseConnectionInfo.getPassword());
        } catch (SQLException e) {
            String message = String.format("Could not open connection for connection string %s", connectionString);
            logger.error(message);

            throw e;
        }
    }

    @Override
    protected String getConnectionString() {
        return String.format("jdbc:mysql://%s:%d/%s", databaseConnectionInfo.getHost(),
                databaseConnectionInfo.getPort(), databaseConnectionInfo.getDatabase());
    }

    @Override
    public boolean tempTableExist(final int sid, final String tableName)
            throws SQLException {
        logger.info(String.format("Getting if temp table exists for sid %d and tablename %s", sid, tableName));

        try (Connection connection = getConnection()) {

            String sql = String.format("SHOW TABLES LIKE 'temp_%s'", tableName);

            try (Statement statement = connection.createStatement()) {

                ResultSet rs = statement.executeQuery(sql);
                if (rs.next()) {
                    return true;
                } else {
                    return false;
                }

            } catch (SQLException e) {
                logger.error(String.format("Getting if temp table exists for sid %d and tablename %s FAILED", sid,
                        tableName), e);

                throw e;
            }
        } catch (SQLException e) {
            logger.info(String.format("FAILED to getting if temp table exists for sid %d and tablename %s", sid,
                    tableName));
            throw e;
        }
    }

    @Override
    public void removeTable(final int sid, final String tableName)
            throws SQLException {
        logger.info(String.format("Removing table for sid %d and tablename %s", sid, tableName));

        try (Connection connection = getConnection()) {

            String sql = String.format("DROP TABLE %s", tableName);

            try (Statement statement = connection.createStatement()) {

                statement.executeUpdate(sql);

            } catch (SQLException e) {
                logger.error(String.format("Removing table for sid %d and tablename %s FAILED", sid, tableName), e);

                throw e;
            }
        } catch (SQLException e) {
            logger.info(String.format("FAILED to removing table for sid %d and tablename %s", sid, tableName));
            throw e;
        }
    }

    @Override
    public void backupOriginalTable(final int sid, final String tableName)
            throws SQLException {

        logger.info(String.format("Backing up table for sid %d and tablename %s", sid, tableName));

        try (Connection connection = getConnection()) {

            String sql = String.format("CREATE TABLE %s SELECT * FROM %s", "temp_" + tableName, tableName);

            try (Statement statement = connection.createStatement()) {

                statement.executeUpdate(sql);

            } catch (SQLException e) {
                logger.error(String.format("Backing up table for sid %d and tablename %s FAILED", sid, tableName), e);

                throw e;
            }
        } catch (SQLException e) {
            logger.info(String.format("FAILED to Backing up table for sid %d and tablename %s", sid, tableName));
            throw e;
        }
    }

    @Override
    public int getColCount(final int sid, final String tableName) throws SQLException {
        logger.info(String.format("Getting column count for sid %d", sid));

        try (Connection connection = getConnection()) {

            String sql = "select count(*) from information_schema.columns where table_schema= ? and table_name= ?";

            try (PreparedStatement statement = connection.prepareStatement(sql)) {
                statement.setString(1, "colfusion_filetodb_" + sid);
                statement.setString(2, tableName);

                int colCount = 0;
                ResultSet rs = statement.executeQuery();
                while (rs.next()) {
                    colCount = Integer.parseInt(rs.getString(1));
                }
                return colCount;

            } catch (SQLException e) {
                logger.error(String.format("Getting column count for sid %d FAILED", sid), e);

                throw e;
            }
        } catch (SQLException e) {
            logger.info(String.format("FAILED to Getting column count for sid %d", sid));
            throw e;
        }
    }
    
    @Override
    public ArrayList<ArrayList<String>> getRows(final String tableName, final int colCount) throws SQLException {
        logger.info(String.format("Getting rows for table %s", tableName));
        ArrayList<ArrayList<String>> rows = new ArrayList<ArrayList<String>>();
        try (Connection connection = getConnection()) {

            String sql = String.format("select * from %s", tableName);

            try (Statement statement = connection.createStatement()) {

                ResultSet rs1 = statement.executeQuery(sql);
                while (rs1.next()) {
                    int colIndex = 1;
                    ArrayList<String> temp = new ArrayList<String>();
                    while (colIndex <= colCount) {
                        temp.add(rs1.getString(colIndex));
                        colIndex++;
                    }
                    rows.add(temp);
                }
                return rows;

            } catch (SQLException e) {
                logger.error(String.format("Getting rows for table %s FAILED", tableName), e);

                throw e;
            }
        } catch (SQLException e) {
            logger.info(String.format("FAILED to getting rows for table %s", tableName));
            throw e;
        }
    }
    @Override
    public void createTable(final int sid, final String tableName)
            throws SQLException {
        logger.info(String.format("Creating table from temp_table for sid %d and tablename %s", sid, tableName));

        try (Connection connection = getConnection()) {

            String sql = String.format("CREATE TABLE %s SELECT * FROM %s", tableName, "temp_" + tableName);

            try (Statement statement = connection.createStatement()) {

                statement.executeUpdate(sql);

            } catch (SQLException e) {
                logger.error(String.format("Creating table from temp_table for sid %d and tablename %s FAILED", sid, tableName), e);

                throw e;
            }
        } catch (SQLException e) {
            logger.info(String.format("FAILED to Creating table from temp_table for sid %d and tablename %s", sid, tableName));
            throw e;
        }
    }

}
