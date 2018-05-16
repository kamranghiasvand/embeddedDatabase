package com.peykasa.embeddeddatabse.domain.repository;

import com.peykasa.embeddeddatabse.config.AppConfig;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Repository;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

/**
 * @author kamran
 */
@Repository
public class SqliteRepository<M,I extends Number> {
    private  Connection connection;
    private AppConfig appConfig;
    @Autowired
    public  SqliteRepository(AppConfig config) throws ClassNotFoundException, SQLException {
        appConfig=config;
        Class.forName("org.sqlite.JDBC");
        connection= DriverManager.getConnection(config.getConnectionString(),config.getDbUser(),config.getDbPass());

    }
    public void  saveAll(Collo)
}
