package edu.pitt.sis.exp.colfusion.dao;
/**
 * @author xxl
 *
 */
public class DatabaseConnectionInfo {
    private String host;
    private int port;
    private String user;
    private String password;
    private String database;
    
    public DatabaseConnectionInfo(String host, int port, String user, String password,
            String database){
        this.host = host;
        this.port = port;
        this.user = user;
        this.password = password;
        this.database = database;
    }
    
    public String getHost() {
        return host;
    }

    public int getPort() {
        return port;
    }

    public String getUser() {
        return user;
    }

    public String getPassword() {
        return password;
    }

    public String getDatabase() {
        return database;
    }
}
