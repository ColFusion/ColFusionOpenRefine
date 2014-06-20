package edu.pitt.sis.exp.colfusion.dao;

public class DatabaseConnectionInfo {
    private final String host;
    private final int port;
    private final String user;
    private final String password;
    private final String database;
    
    public DatabaseConnectionInfo(final String host, final int port, final String user, final String password,
            final String database){
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
