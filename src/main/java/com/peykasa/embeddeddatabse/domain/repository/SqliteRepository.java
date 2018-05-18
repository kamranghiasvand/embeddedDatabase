package com.peykasa.embeddeddatabse.domain.repository;

import com.peykasa.embeddeddatabse.config.AppConfig;
import com.peykasa.embeddeddatabse.domain.model.ReportModel;
import org.apache.ddlutils.PlatformUtils;
import org.apache.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.BatchPreparedStatementSetter;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.TransactionStatus;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.transaction.support.TransactionTemplate;

import javax.sql.DataSource;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.List;

/**
 * @author kamran
 */
@SuppressWarnings("SqlNoDataSourceInspection")
@Service
public class SqliteRepository {
    private final static Logger LOGGER = Logger.getLogger(SqliteRepository.class);
    private long lastId;
    private JdbcTemplate template;
    private TransactionTemplate transactionTemplate;
    private String databaseType;

    @Autowired
    public SqliteRepository(JdbcTemplate jdbcTemplate, TransactionTemplate transactionTemplate) throws ClassNotFoundException, SQLException {
        template = jdbcTemplate;
        databaseType = databaseType(template.getDataSource()).toLowerCase();
        this.transactionTemplate = transactionTemplate;
        try {
            String cmd = "CREATE TABLE  records (id bigint,counter int,current_cell int,current_lac int,current_vlr bigint,event_type VARCHAR(100) )";
            template.execute(cmd);
        } catch (Exception ex) {
            LOGGER.error(ex.getMessage(), ex);
        }
        try {
            lastId = template.queryForObject("SELECT MAX(id) max_id FROM records", Long.class);

        } catch (Exception ex) {
            lastId = 0;
        }
    }


    public void insertBulk(List<ReportModel> entities) throws SQLException {

           String cmd = "INSERT INTO records (id,counter,current_cell,current_lac,current_vlr,event_type) VALUES(?,?,?,?,?,?) ";
        try {
            transactionTemplate.execute((TransactionStatus status) -> {
                try {
                    template.batchUpdate(cmd, new BatchPreparedStatementSetter() {
                        @Override
                        public void setValues(PreparedStatement ps, int i) throws SQLException {
                            ps.setLong(1, (++lastId));
                            ps.setLong(2, entities.get(i).getCounter());
                            ps.setLong(3, entities.get(i).getCurrentCell());
                            ps.setLong(4, entities.get(i).getCurrentLAC());
                            ps.setLong(5, entities.get(i).getCurrentVLR());
                            ps.setString(6, entities.get(i).getEventType());
                        }

                        @Override
                        public int getBatchSize() {
                            return entities.size();
                        }
                    });
                } catch (Exception ex) {
                    LOGGER.error(ex.getMessage(), ex);
                }
                return 0;
            });
        } catch (Exception ex) {
            LOGGER.error(ex.getMessage(), ex);
        }
    }

    public long updateBulk(List<ReportModel> entities) throws SQLException {

        String cmd = "UPDATE records SET counter=? ,current_cell=?, current_lac=? ,current_vlr=? ,event_type=? where id=?";
        try {
            transactionTemplate.execute((TransactionStatus status) -> {
                try {
                    template.batchUpdate(cmd, new BatchPreparedStatementSetter() {
                        @Override
                        public void setValues(PreparedStatement ps, int i) throws SQLException {
                            ps.setLong(1, entities.get(i).getCounter());
                            ps.setLong(2, entities.get(i).getCurrentCell());
                            ps.setLong(3, entities.get(i).getCurrentLAC());
                            ps.setLong(4, entities.get(i).getCurrentVLR());
                            ps.setString(5, entities.get(i).getEventType());
                            ps.setLong(6, entities.get(i).getId());
                        }

                        @Override
                        public int getBatchSize() {
                            return entities.size();
                        }
                    });
                } catch (Exception ex) {
                    LOGGER.error(ex.getMessage(), ex);
                }
                return 0;
            });

        } catch (Exception ex) {
            LOGGER.error(ex.getMessage(), ex);
        }
        return entities.size();
    }

    public void clearTable() {
        template.execute("DELETE FROM records");
        try {
            template.execute("vacuum");
        } catch (Exception ex) {
            LOGGER.error(ex.getMessage(), ex);
        }
    }

    @Transactional(readOnly = true)
    public List<ReportModel> fetch(long minId, long maxId) {

        String cmd = "SELECT id,counter,current_cell,current_lac,current_vlr,event_type FROM records where id BETWEEN ? and ?";
        return template.query(cmd, new Object[]{minId, maxId}, (rs, rNum) ->
                new ReportModel(rs.getLong("id"), rs.getInt("counter"), rs.getLong("current_vlr"),
                        rs.getInt("current_lac"), rs.getInt("current_cell"), rs.getString("event_type")));

    }

    public long getMinId() {
        try {
            return template.queryForObject("SELECT MIN(id) FROM records", Long.class);

        } catch (Exception ex) {
            LOGGER.error(ex.getMessage(), ex);
            return 0;
        }
    }

    public Long deleteBatch(long min, long max) {
        String cmd = "DELETE FROM records where id between ? and ?";
        try {
            transactionTemplate.execute((TransactionStatus status) -> {
                try {
                    template.batchUpdate(cmd, new BatchPreparedStatementSetter() {
                        @Override
                        public void setValues(PreparedStatement ps, int i) throws SQLException {
                            ps.setLong(1, min);
                            ps.setLong(2, max);
                        }

                        @Override
                        public int getBatchSize() {
                            return (int) (max - min);
                        }
                    });
                } catch (Exception ex) {
                    LOGGER.error(ex.getMessage(), ex);
                }
                return 0;
            });

        } catch (Exception ex) {
            LOGGER.error(ex.getMessage(), ex);
        }
        return max - min;
    }

    private String databaseType(DataSource dataSource) {
        return new PlatformUtils().determineDatabaseType(dataSource);
    }
}
