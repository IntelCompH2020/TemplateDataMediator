package com.citesa.intelcomp.templatedatamediator;

//import com.fasterxml.jackson.core.JsonProcessingException;
//import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.log4j.Logger;
import org.apache.spark.sql.*;
import org.apache.spark.sql.SparkSession.Builder;

public class Driver {
    public static final String appName = "SparkSQLTemplate Data Mediator";
    public static final String appId = "SparkSQLTemplateDataMediator";
    public static final String appReleaseId = "v1.0.0000 06 June 2022";

    public static void main(String[] args) throws Exception {

        Logger logger = Logger.getLogger("MainLogger");


        /* Preparing operation environment for the execution */
        Arguments opArguments = new Arguments(args);

        Environment opEnv = null;
        SparkSession spark = null;

                //Preparing processing components
        opEnv = new Environment(opArguments,appId,logger);
        Query query = new Query(opEnv);


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


            Dataset<Row> dsOut = query.getDataSet(spark);

            query.StoreDataSet(spark, dsOut);

        }
        finally
        {
            if(spark!=null)
                spark.close();
        }
    }
}
