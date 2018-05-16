package com.peykasa.embeddeddatabse;

import com.peykasa.embeddeddatabse.service.EvaluatorService;
import org.apache.log4j.BasicConfigurator;
import org.apache.log4j.Logger;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.ConfigurableApplicationContext;

import java.sql.SQLException;

@SpringBootApplication
public class EmbeddedDatabaseApplication {
    private final static Logger logger = Logger.getLogger(EmbeddedDatabaseApplication.class);

    public static void main(String[] args) throws SQLException, ClassNotFoundException {
        try {
            BasicConfigurator.configure();
            ConfigurableApplicationContext application = SpringApplication.run(EmbeddedDatabaseApplication.class, args);

            EvaluatorService evaluator = application.getBean(EvaluatorService.class);
            evaluator.start();
        } catch (Exception ex) {
            logger.error(ex.getMessage(), ex);
        }
    }
}
