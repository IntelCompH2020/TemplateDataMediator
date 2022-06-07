package com.citesa.intelcomp.templatedatamediator;

import com.citesa.trivials.NotImplementedException;
import com.citesa.trivials.io;
import com.citesa.intelcomp.infrahelper.SimpleFileReader;
import com.citesa.intelcomp.infrahelper.SimpleFileReaderBase;
import com.citesa.spark.sqlcomposer.*;

import com.citesa.trivials.string;
import com.citesa.trivials.types.Severity;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import org.w3c.dom.*;

import java.io.IOException;
import java.net.URISyntaxException;
import java.util.HashMap;

public class Query {
    protected com.citesa.intelcomp.templatedatamediator.Environment _opEnv;

    public Query(Environment opEnv)
    {
        this._opEnv = opEnv;
    }

    public Dataset<Row> getDataSet(SparkSession spark) throws Exception {

        SparkSqlTemplate sprkSqlTmpl = GetSparkSqlTemplate();

        HashMap<String,String> args = _opEnv.getArguments().getSupplementaryArguments();
        for (String key: args.keySet()) {
            try {
                String value = args.get(key);
                sprkSqlTmpl.addArgument( key, value );
            }
            catch(IllegalArgumentException exIllegalArg)
            {
                //ignore the exception
            }
        }

        //Prepare data sources
        for (DataSource ds : sprkSqlTmpl.dataSources.values()) {
           PrepareDataSource(spark, ds);
        }

        //Prepare data sinks
        for (DataSink ds : sprkSqlTmpl.dataSinks.values()) {
            PrepareDataSink(spark, ds);
        }

        HashMap<String, Dataset<Row>>  datasets = sprkSqlTmpl.PrepareAll(spark);
        Dataset<Row> dataset = null;
        for (String key :
             datasets.keySet()) {
            dataset = datasets.get((key));
            if (dataset != null) {
                //TODO: handle multi-datasets. Currently only the first non-null dataset is retrieved by StandardQuery
                break;
            }
        }
        return dataset;
    }
    private void PrepareDataSink(SparkSession spark, DataSink ds)
    {
        //TODO: add code to prepare datasink parameters.
        //      Currenty DataSinks are not utilized as the storage is tackled by the Mediator logic,
        //      i.e. not the one of the sink
    }
    private void PrepareDataSource(SparkSession spark, DataSource ds)
    {
        //TODO: add more code to prepare datasource parameters
        //locating dataset by id
        com.citesa.intelcomp.cataloguehelper.DatasetInstanceBase dataSetInstance = null;
        if(!string.isNullOrEmpty(ds.dataType))
        {
            if(!ds.dataType.equals(dataSetInstance.getDatasetType()))
            {
                throw new IllegalArgumentException("Dataset type requested as '"+ ds.dataType+"' but instead '"+dataSetInstance.getDatasetType() +"' was found in the catalogue!");
            }
        }
        if(dataSetInstance != null )
            ds.location = dataSetInstance.getRefLocation();
    }

    private SparkSqlTemplate GetSparkSqlTemplate() throws URISyntaxException, IOException {

        SimpleFileReader fr =  SimpleFileReaderBase.getFileReader(
                io.PathToURI( _opEnv.getArguments().queryTemplate)
        );
        String xmlText = fr.getFileContentString();
        Document xmlDoc =  com.citesa.trivials.xml.xmlDocFromString(xmlText);
        Element docElement = xmlDoc.getDocumentElement();
        return  new SparkSqlTemplate( docElement);
    }

    public void StoreDataSet(SparkSession spark, Dataset<Row> dsOut) {
        throw new NotImplementedException();
    }
}