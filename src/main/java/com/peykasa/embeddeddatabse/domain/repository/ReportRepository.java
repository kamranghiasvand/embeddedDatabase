package com.peykasa.embeddeddatabse.domain.repository;

import com.peykasa.embeddeddatabse.domain.model.ReportModel;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.data.jpa.repository.support.SimpleJpaRepository;
import org.springframework.stereotype.Repository;
import org.springframework.transaction.annotation.Transactional;

import javax.persistence.EntityManager;
import javax.persistence.PersistenceContext;
import java.util.Collection;

/**
 * @author kamran
 */
@Repository
@Transactional
public class ReportRepository extends SimpleJpaRepository<ReportModel,Long>{
    private final static Logger LOGGER = LogManager.getLogger(ReportRepository.class);
    @PersistenceContext
    private EntityManager entityManager;
    @Value("${hibernate.jdbc.batch_size:10}")
    private int batchSize;

    @Autowired
    public ReportRepository(EntityManager entityManager) {
        super(ReportModel.class,entityManager);
        this.entityManager = entityManager;

    }
    public <T extends ReportModel> void bulkSave(Collection<T> entities) {
        try {
//            int i = 0;
            for (T t : entities) {
                if (t.getId() == 0)
                    entityManager.persist(t);
                else entityManager.merge(t);
//                i++;
//                if (i % batchSize == 0) {
//                    entityManager.flush();
//                    entityManager.clear();
//                }
            }
            entityManager.flush();
            entityManager.clear();
        } catch (Exception ex) {
            LOGGER.error(ex.getMessage(), ex);
        }
    }
}
