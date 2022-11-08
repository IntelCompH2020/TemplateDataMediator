package com.citesa.intelcomp.sparksqltemplatedatamediator;

import com.citesa.intelcomp.cataloguehelper.CatalogueAccess;
import com.citesa.intelcomp.cataloguehelper.DatasetInstanceBase;
import com.citesa.intelcomp.cataloguehelper.DatasetResource;
import com.citesa.intelcomp.clienttoolkit.ProgramArgumentsBase;
import com.citesa.trivials.NotImplementedException;
import com.citesa.trivials.config.ConfigXml;
import com.citesa.trivials.io;
import com.citesa.intelcomp.infrahelper.SimpleFileReader;
import com.citesa.intelcomp.infrahelper.SimpleFileReaderBase;
import com.citesa.spark.sqlcomposer.*;

import com.citesa.trivials.string;
import com.citesa.trivials.types.KeyValuePair;
import org.apache.log4j.Level;
import org.apache.log4j.Priority;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import org.w3c.dom.*;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;

public class Query {
    protected com.citesa.intelcomp.sparksqltemplatedatamediator.Environment _opEnv;

    public Query(Environment opEnv)
    {
        this._opEnv = opEnv;
    }

    public void ExecuteQuery(SparkSession spark) throws Exception {

        //Read template
        SparkSqlTemplate sparkSqlTemplate = InitSparkSqlTemplate();

        //Pass command line arguments to query engine
        ArrayList<KeyValuePair<String,String>> args  =this._opEnv.getArguments().ParseQueryArguments();
        for (KeyValuePair<String,String> arg: args ) {
            Logging.getLog().logF(Level.INFO,"CLI query argument [%s] = [%s]", arg.key, arg.value);
            if(!string.isNullOrEmpty((arg.key)))
                sparkSqlTemplate.addArgument(arg.key, arg.value);
        }

        //Prepare data sources
        for (DataSource ds : sparkSqlTemplate.dataSources.values()) {
            PrepareDataSource(spark, ds);
        }

        //Prepare data sinks
        for (DataSink ds : sparkSqlTemplate.dataSinks.values()) {
            PrepareDataSink(spark, ds);
        }

        sparkSqlTemplate.Execute(spark);
        //HashMap<String, Dataset<Row>>  datasets = sparkSqlTemplate.PrepareAll(spark);
        //sparkSqlTemplate.DepositData(spark , datasets);
    }
    private void PrepareDataSink(SparkSession spark, DataSink ds)
    {
        //TODO: add code to prepare datasink parameters.
        Logging.getLog().logF(Level.INFO, "Preparing DataSink #%d [%s]",ds.tmpOrdinal, ds.id);
        ArrayList<KeyValuePair<String,String>> outlocations = _opEnv.getArguments().ParseOutputLocations();
        String location = ProgramArgumentsBase.GetParsedArgumentValueByKeyOrOrdinal(outlocations,ds.id, ds.tmpOrdinal);
        if(location == null )
        {
            Logging.getLog().logF(Level.INFO,
                    "DataSink [%s] (#%d) location is not declared as argument. Provided [%s] is used.",ds.id, ds.tmpOrdinal, ds.location );
        }
        else
        {
            ds.location = location;
            Logging.getLog().logF(Level.INFO,
                    "DataSink [%s] (#%d) location is set to [%s].",ds.id, ds.tmpOrdinal, ds.location );
        }
    }
    private void PrepareDataSource(SparkSession spark, DataSource ds)
    {
        //TODO: add code to prepare datasource parameters
        String location = null;
        Logging.getLog().logF(Level.INFO, "Preparing DataSource #%d [%s]. Name: [%s]. Store Type: [%s]. Data Type: [%s].  ",ds.tmpOrdinal, ds.id, ds.name, ds.storeType, ds.dataType);
        ArrayList<KeyValuePair<String,String>> dsIds = _opEnv.getArguments().ParseDatasetIDs();
        //locating dataset by id
        if(dsIds.size()>0)
        {
            String datasetId = null;
            if(dsIds.size() == 1  )
                datasetId = dsIds.get(0).value;
            else {
                datasetId = ProgramArgumentsBase.GetParsedArgumentValueByKeyOrOrdinal(
                    dsIds, ds.id, ds.tmpOrdinal
                );
            }
            if(!string.isNullOrEmpty( datasetId)) {
                Logging.getLog().logF(Level.INFO, "DataSource [%s] (#%d) is assigned to dataset [%s].", ds.id,ds.tmpOrdinal, datasetId);
                com.citesa.intelcomp.cataloguehelper.DatasetInstanceBase dataSetInstance = null;
                Class datasetInstanceClass =null;
                try {
                    if(!string.isNullOrEmpty(ds.dataType))
                        datasetInstanceClass = Class.forName(ds.dataType);
                }
                catch (Exception ex) {
                    Logging.getLog().logAndPrintException(Level.WARN, ex, "Could not instantiate %s datatype of DataSource [%s] for reading catalogue dataset.", ds.dataType, ds.id);
                }
                if(datasetInstanceClass == null) {
                    datasetInstanceClass = com.citesa.intelcomp.cataloguehelper.datasettypes.GenericFileFolderBasedDataset.class;
                    Logging.getLog().logF(Level.WARN, "No datatype was declared for DataSource [%s].", ds.id);
                }
                dataSetInstance = (DatasetInstanceBase) new CatalogueAccess(_opEnv.getConfigManager())
                        .getDatasetDescription(
                                datasetInstanceClass,
                                datasetId);

                //todo: the current implementation copies only the location from the dataset instance, however it should be using other info too (e.g. credential pointers ? )
                location = dataSetInstance.getRefLocation();
            }
        }
        if(location == null)
        {
            location = ProgramArgumentsBase.GetParsedArgumentValueByKeyOrOrdinal(
                    _opEnv.getArguments().ParseDatasetLocations(), ds.id, ds.tmpOrdinal
            );
        }
        if(location!=null) {
            ds.location = location;
            Logging.getLog().logF(Level.INFO, "DataSource [%s] (#%d) location is set to [%s]", ds.id, ds.tmpOrdinal, ds.location);
        }
        else
        {
            Logging.getLog().logF(Level.WARN, "No location was identified for DataSource [%s] (#%d)", ds.id, ds.tmpOrdinal);
        }
    }

    private SparkSqlTemplate InitSparkSqlTemplate() throws URISyntaxException, IOException {

        Logging.getLog().logF(Level.INFO, "Reading query template from [%s]", _opEnv.getArguments().queryTemplateFile);
        SimpleFileReader fr =  SimpleFileReaderBase.getFileReader(
                io.PathToURI( _opEnv.getArguments().queryTemplateFile)
        );
        String xmlText = fr.getFileContentString();
        Document xmlDoc =  com.citesa.trivials.xml.xmlDocFromString(xmlText);
        Element docElement = xmlDoc.getDocumentElement();
        SparkSqlTemplate tmpl =  new SparkSqlTemplate( docElement);

        String configFile = _opEnv.getArguments().queryTemplateArgumentsFile;
        if(!string.isNullOrEmpty(configFile)) {

            if (configFile.endsWith(".xml")) {
                ConfigXml xCfg = null;

                xCfg = new ConfigXml();
                Logging.getLog().logF(Level.INFO, "Reading query arguments from [%s]", configFile);
                xCfg.Load(SimpleFileReaderBase.readAllFileText(new URI(configFile)));

                Collection<String> fArgs = xCfg.getNodeNames("queryArguments");
                for (String arg : fArgs) {
                    String value = xCfg.getValue("queryArguments/" + arg);
                    Logging.getLog().logF(Level.INFO, "Template Argument from file: [%s] =  %s", arg, value);
                    tmpl.getArguments().put(arg, value);
                }
                //arg_explain = (xCfg.getValue("programArguments/explain") =="1");
            }
        }
        return tmpl ;
    }

}