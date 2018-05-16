package com.peykasa.embeddeddatabse.domain.repository;

import com.peykasa.embeddeddatabse.config.AppConfig;
import com.peykasa.embeddeddatabse.domain.model.ReportModel;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.sql.*;
import java.util.Collection;

/**
 * @author kamran
 */
@Service
public class SqliteRepository {
    private Connection connection;
    private AppConfig appConfig;
    private long lastId;

    @Autowired
    public SqliteRepository(AppConfig config) throws ClassNotFoundException, SQLException {
        appConfig = config;
        Class.forName("org.sqlite.JDBC");
        connection = DriverManager.getConnection(config.getConnectionString(), config.getDbUser(), config.getDbPass());
        connection.setAutoCommit(false);
        String cmd = "CREATE TABLE IF NOT EXISTS `records` (\n" +
                "  `id` bigint(20) NOT NULL,\n" +
                "  `count` int(11) DEFAULT NULL,\n" +
                "  `current_cell` int(11) DEFAULT NULL,\n" +
                "  `current_lac` int(11) DEFAULT NULL,\n" +
                "  `current_vlr` bigint(20) DEFAULT NULL,\n" +
                "  `event_type` tinyint(4) DEFAULT NULL\n" +
                ")";

        Statement statement = connection.createStatement();
        statement.execute(cmd);
        statement.close();
        connection.commit();
        try {
            statement = connection.createStatement();
            ResultSet rs = statement.executeQuery("SELECT MAX(id) max_id FROM records;");
            if (rs.next()) {
                Long id = rs.getLong("max_id");
                if (id != null)
                    lastId = id;
            }
            rs.close();
            statement.close();
            connection.commit();
        } catch (Exception ex) {
            lastId = 0;
        }
    }

    public void insertBulk(Collection<ReportModel> entities) throws SQLException {

        String cmd = "INSERT INTO records (id,count,current_cell,current_lac,current_vlr,event_type) VALUES ";

        for (ReportModel m :
                entities) {
            cmd += "(" + (++lastId) + "," + m.getCount() + "," + m.getCurrentCell() + "," + m.getCurrentLAC() + "," + m.getCurrentVLR() + "," + m.getEventType() + "),";
        }
        cmd = cmd.substring(0, cmd.length() - 1) + ";";
        Statement statement = connection.createStatement();
        statement.execute(cmd);
        statement.close();
        connection.commit();
    }
}
