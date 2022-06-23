package com.citesa.intelcomp.sparksqltemplatedatamediator;

//import com.fasterxml.jackson.core.JsonProcessingException;
//import com.fasterxml.jackson.databind.ObjectMapper;
import com.citesa.intelcomp.Logging;
import com.citesa.trivials.logging.Log;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.*;
import org.apache.spark.sql.SparkSession.Builder;

public class Driver {
    public static final String appName = "SparkSQLTemplate Data Mediator";
    public static final String appId = "SparkSQLTemplateDataMediator";
    public static final String appReleaseId = "v1.0.0000 06 June 2022";

    public static void main(String[] args) throws Exception {

        Logger logger = Logger.getLogger("SparkSqlTemplateDataMediator");
        logger.setLevel(Level.INFO); //Initial level: INFO
        Logging.setLog(new Log(logger));

        /* Preparing operation environment for the execution */
        Arguments opArguments = new Arguments(args);

        Environment opEnv = null;
        SparkSession spark = null;

                //Preparing processing components
        opEnv = new Environment(opArguments, appId);
        logger.setLevel(Log.toLogLevel( opEnv.getLogLevel())); //Initial level: INFO
        Query query = new Query(opEnv);

        Logging.getLog().logF(Level.INFO, "======Starting %s driver execution======", appId);

        //String sparkMaster = opEnv.getSparkMaster();
        try
        {
            Builder  builder = SparkSession.builder()
                    .appName(opEnv.getApplicationTitle(appName));
            spark = builder.getOrCreate();
            if(opEnv.getLogLevel()!=null)
            {
                spark.sparkContext().setLogLevel(opEnv.getLogLevel());
            }

            Logging.getLog().logF(Level.INFO, "======Starting %s query execution as application [%s] ======", appId, appId);
            query.ExecuteQuery(spark);
        }
        finally
        {
            if(spark!=null) {
                Logging.getLog().logF(Level.INFO, "Closing open SparkSession");
                spark.close();
            }
        }
    }
}
