package com.citesa.intelcomp.sparksqltemplatedatamediator;


import com.citesa.intelcomp.clienttoolkit.OperationEnvironmentBase;
import com.citesa.trivials.NotImplementedException;
import com.citesa.trivials.io;
import com.citesa.trivials.types.LogMessage;
import org.apache.log4j.Logger;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.UUID;

public class Environment extends OperationEnvironmentBase  {

	public ArrayList<LogMessage> PrintInfo(boolean onlyLocal) {
		ArrayList<LogMessage> info = new ArrayList<>();
		info.add( new LogMessage("----Operation Environment----"));
		info.add( new LogMessage("Config : " + getPointZeroRef()));
		info.add( new LogMessage("Catalogue : " + getConfigManager().getCatalogueEndpoint()));
		/*
		info.add( new LogMessage("Dataset Id : " + getDatasetId()));
		info.add( new LogMessage("Dataset Location: " + getDatasetFSLocation()));
		info.add( new LogMessage("Output Location : " + getOutputLocation()));
		info.add( new LogMessage("Output Mode : " + getOutputMode()));
		*/
		if(!onlyLocal)
			info.addAll(getArguments().PrintInfo( false ));

		return info;
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
