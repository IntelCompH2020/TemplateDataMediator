package com.citesa.intelcomp.sparksqltemplatedatamediator;


import com.citesa.intelcomp.clienttoolkit.OperationEnvironmentBase;
import com.citesa.trivials.NotImplementedException;
import com.citesa.trivials.io;
import com.citesa.trivials.logging.Log;
import com.citesa.trivials.types.LogMessage;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.UUID;

public class Environment extends OperationEnvironmentBase  {

	public void LogInfo(boolean noCascading, Level level, Log logger){
		logger.logF(level,"----Operation Environment----");
		logger.logF(level,"Config : [%s]" , getPointZeroRef());
		logger.logF(level,"Catalogue : [%s]" , getConfigManager().getCatalogueEndpoint());
		/*
		logger.logF(level,"Dataset Id : " + getDatasetId()));
		logger.logF(level,"Dataset Location: " + getDatasetFSLocation()));
		logger.logF(level,"Output Location : " + getOutputLocation()));
		logger.logF(level,"Output Mode : " + getOutputMode()));
		*/
		if(!noCascading)
			getArguments().LogInfo( false, level,logger );

	}

	public Environment(Arguments arguments, String appId) {
		super(arguments, appId);
	}

	public Arguments getArguments()
	{
		return (Arguments) super.getArguments();
	}

	public String getHDFSTmpPath() {
		return getHDFSTmpPath("");
	}
	public String getLocalTmpPath() {
		return io.concatenatePaths(
				System.getProperty("java.io.tmpdir"),
				UUID.randomUUID().toString(),
				"/"
			);
	}

	public String getTempPath(String workLocation)
	{
		if(getArguments().tmpLocation != null)
			return io.concatenatePaths(
					getArguments().tmpLocation,
					UUID.randomUUID().toString(),
					"/");

		if(io.isPathHdfs(workLocation))
			return getHDFSTmpPath();
		if(io.isPathLocal(workLocation))
			return getLocalTmpPath();
		return null;
	}

	public String getHDFSTmpPath(String seed)
	{
		if(seed==null)
			seed="";
		return
				io.concatenatePaths(
						this.getConfigManager().getHDFSEndpoint(),
						this.getConfigManager().getHDFSTmpBasePath(),
						DateTimeFormatter.ofPattern("yyyy-MM-dd-HHmmss").format(LocalDateTime.now())+seed,
						"/");
	}
}
