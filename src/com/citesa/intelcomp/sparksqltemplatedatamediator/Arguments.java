package com.citesa.intelcomp.sparksqltemplatedatamediator;


import com.citesa.intelcomp.clienttoolkit.ProgramArgumentsBase;
import com.citesa.intelcomp.infrahelper.SimpleFileReaderBase;
import com.citesa.trivials.config.ConfigJson;
import com.citesa.trivials.config.ConfigXml;
import com.citesa.trivials.string;
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


    @Option(name = "-bl", usage = "Base location(s) override")
    public List<String> baseLocations = null;

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

    protected Hashtable<String, String> _argumentsFromFile = null;
    /**
     * Retrieves the arguments provided in the json file declared in queryArgumentsFile
     * @return a dictionary with the arguments
     */
    public Hashtable<String, String > getArgumentsFromFile()
    {
        if(_argumentsFromFile !=null)
            return _argumentsFromFile;

        if(string.isNullOrEmpty(queryArgumentsFile))
            return null;

        _argumentsFromFile = new Hashtable<>();
        String configPayload = null;
        try {
            configPayload = SimpleFileReaderBase.readAllFileText( new URI(queryArgumentsFile));
        } catch (IOException e) {
            e.printStackTrace();
        } catch (URISyntaxException e) {
            e.printStackTrace();
        }

        if(queryArgumentsFile.endsWith(".json")) {
            ConfigJson conf = new ConfigJson();
            conf.Load(configPayload);
            Collection<String> argNames = conf.getNodeNames();
            if (argNames != null) {
                for (String argName : argNames) {
                    if (argName != null)
                        _argumentsFromFile.put(argName, conf.getValueString(argName));
                }
            }
        }
        else if(queryArgumentsFile.endsWith(".xml")) {
            ConfigXml conf = new ConfigXml();
            conf.Load(configPayload);
            Collection<String> argNames = conf.getNodeNames();
            if (argNames != null) {
                for (String argName : argNames) {
                    if (argName != null)
                        _argumentsFromFile.put(argName, conf.getValue(argName));
                }
            }
        }
        return _argumentsFromFile;
    }

    protected Hashtable<String, String> _argumentsFromList = null;
    /**
     * Retrieves the arguments provided in queryArgumentsList
     * @return a dictionary with the arguments
     */
    public Hashtable<String, String> getArgumentsFromList()
    {
        if(_argumentsFromList!=null)
            return  _argumentsFromList;

        if(string.isNullOrEmpty(queryArgumentsList))
            return null;

        String[] args = queryArgumentsList.split("\\|\\|");
        for (String arg : args) {
            if(!string.isNullOrEmpty(arg))
            {
                String parts[] = arg.split("=",2);
                if(parts.length == 2) {
                    _argumentsFromList.put(parts[0].trim().toLowerCase(),parts[1]);
                }
            }

        }
        return _argumentsFromList;
    }

    /**
     * Retrieves the value of an argument provided in a file or in argument list
     * Arguments in argument list take precedence over arguments in file.
     * @param name the param to look for (case insensitive)
     * @param defaultValue the value to return if no argument is found
     * @return the string value of the argument or the defaultValue
     */
    public String getSupplementaryArgumentValue(String name, String defaultValue)
    {
        try
        {
            String v = getArgumentsFromList().get(name.toLowerCase());
            if(v!=null)
                return v;
        }
        catch(Exception ex)
        {

        }

        try
        {
            String v = getArgumentsFromFile().get(name.toLowerCase());
            if(v!=null)
                return v;
        }
        catch(Exception ex)
        {

        }
        return defaultValue;
    }

    protected HashMap<String,String> _supplementaryArguments = null;
    public HashMap<String,String> getSupplementaryArguments() {

        if(_supplementaryArguments!=null)
            return _supplementaryArguments;

        Hashtable<String, String> listArgs = getArgumentsFromList();
        Hashtable<String, String> fileArgs = getArgumentsFromFile();
        _supplementaryArguments = new HashMap<String, String>();

        if(listArgs!=null) {
            while(listArgs.keys().hasMoreElements()) {
                String key = listArgs.keys().nextElement();
                _supplementaryArguments.put(key, listArgs.get(key));
            }
        }

        if(fileArgs!=null) {
            while(fileArgs.keys().hasMoreElements()) {
                String key = fileArgs.keys().nextElement();
                try {
                    String v =  null;
                    v = listArgs.get(key);
                    if(v != null)
                        _supplementaryArguments.put(key, v);
                }
                catch  (Exception ex)
                {

                }
            }
        }

        return _supplementaryArguments;
    }


    public ArrayList<LogMessage> PrintInfo(boolean onlyLocal) {
        ArrayList<LogMessage> info = new ArrayList<>();
        info.add( new LogMessage("----Core Arguments----"));
        info.add( new LogMessage("Dataset Id : " + string.asPrintable( this.datasetId ) ));
        info.add( new LogMessage("Dataset FS Location: " + string.asPrintable( this.datasetLocation) ));
        info.add( new LogMessage("Output Location : " + string.asPrintable( this.outputLocation)));
        info.add( new LogMessage("Output Mode : " + string.asPrintable( this.outMode )));
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
