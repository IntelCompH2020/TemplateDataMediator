package com.citesa.intelcomp.sparksqltemplatedatamediator;


import com.citesa.intelcomp.clienttoolkit.ProgramArgumentsBase;
import com.citesa.intelcomp.infrahelper.SimpleFileReaderBase;
import com.citesa.trivials.config.ConfigJson;
import com.citesa.trivials.config.ConfigXml;
import com.citesa.trivials.string;
import com.citesa.trivials.types.KeyValuePair;
import com.citesa.trivials.types.LogMessage;
import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.Option;
import org.kohsuke.args4j.spi.StringArrayOptionHandler;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.*;

public class Arguments extends ProgramArgumentsBase {

    public Arguments(String[] args) throws CmdLineException {
        super(args);
        this.ParseAnnotatedArgs(args);
    }

    /* Data input options */
    @Option(name = "-dsmap", handler = StringArrayOptionHandler.class, usage = "Dataset mapping (if absent, the ordinal is utilized)")
    public List<String> datasetMap = null;

    @Option(name = "-ds", handler = StringArrayOptionHandler.class, usage = "Dataset Id(s)")
    public List<String> datasetIds = null;

    @Option(name = "-fsds", handler = StringArrayOptionHandler.class, usage = "Dataset Location(s)")
    public List<String> datasetLocations = null;


    /* Data output options */
    @Option(name = "-ol", aliases = "--outlocations", handler = StringArrayOptionHandler.class, usage = "Output Location (kafka endpoint, hdfs location, local path")
    public List<String> outputLocations = null;

    @Option(name = "-oo", aliases = "--outoption", handler = StringArrayOptionHandler.class,  usage = "Output Options (utilized according to output mode). Format: option=value")
    public List<String> outputOptions = null;

    /**
     * A file containing query template arguments. The format is defined by the SparkSQLComposer component.
     */
    @Option(name = "-qta", usage = "Query template arguments file")
    public String queryTemplateArgumentsFile = null;

    /**
     * A file containing query template. The format is defined by the SparkSQLComposer component.
     */
    @Option(name = "-qt", usage = "Query Template File")
    public String queryTemplateFile = "";

    /**
     * Additional query arguments supplied in the form arg_name=value
     * Arguments supplied via the method are passed to the SparkSQLComposer in addition to the template file.
     */
    @Option(name = "-qa",handler = StringArrayOptionHandler.class, usage = "Query Arguments in format argument_name=value")
    public List<String> queryArguments = null;


    @Option(name = "-reg", usage = "Register Output")
    public boolean registerOut = false;

    @Option(name = "-tmp", usage = "Temporary files base location")
    public String tmpLocation = null;

    @Option(name = "-explain",usage = "Explain query")
    public String explainQuery = null;



    protected  ArrayList<KeyValuePair<String, String>> _parsedQueryArguments;
    public ArrayList<KeyValuePair<String, String>> ParseQueryArguments() {
        if(_parsedQueryArguments == null)
            _parsedQueryArguments =  ProgramArgumentsBase.ParseCombinedArguments(this.queryArguments);
        return _parsedQueryArguments;
    }

    protected  ArrayList<KeyValuePair<String, String>> _parsedDatasetIds;
    public ArrayList<KeyValuePair<String, String>> ParseDatasetIDs() {
        if( _parsedDatasetIds == null )
            _parsedDatasetIds =  ProgramArgumentsBase.ParseCombinedArguments(this.datasetIds);
        return _parsedDatasetIds;
    }


    protected  ArrayList<KeyValuePair<String, String>> _parsedDatasetLocations;
    public ArrayList<KeyValuePair<String, String>> ParseDatasetLocations() {
        if(_parsedDatasetLocations == null)
            _parsedDatasetLocations = ProgramArgumentsBase.ParseCombinedArguments(this.datasetLocations);
        return _parsedDatasetLocations;
    }

    protected ArrayList<KeyValuePair<String, String>> _parsedOutputLocations;
    public ArrayList<KeyValuePair<String, String>> ParseOutputLocations() {
        if(_parsedOutputLocations == null)
            _parsedOutputLocations  = ProgramArgumentsBase.ParseCombinedArguments(this.outputLocations);
        return _parsedOutputLocations;
    }

    public ArrayList<LogMessage> PrintInfo(boolean onlyLocal) {
        ArrayList<LogMessage> info = new ArrayList<>();
        info.add( new LogMessage("----Core Arguments----"));
        /*
        info.add( new LogMessage("Dataset Id : " + string.asPrintable( this.datasetId ) ));
        info.add( new LogMessage("Dataset FS Location: " + string.asPrintable( this.datasetLocation) ));
        info.add( new LogMessage("Output Location : " + string.asPrintable( this.outputLocation)));
        info.add( new LogMessage("Output Mode : " + string.asPrintable( this.outMode )));
         */
        return  info;
    }

    public String getQueryExplanation() {
        if(explainQuery != null) {
            explainQuery = explainQuery.toLowerCase().trim();
            switch (explainQuery) {
                case "simple":
                case "extended":
                case "codegen":
                case "cost":
                case "formatted":
                    return explainQuery;
            }
        }
        return null;
    }

}
