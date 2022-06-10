package com.citesa.intelcomp.sparksqltemplatedatamediator;

import com.citesa.trivials.logging.Log;

public class Logging {
    private static Log _log;

    public static Log getLog() {
        return com.citesa.intelcomp.Logging.getLog();
    }

    public static void setLog(Log log) {
        com.citesa.sqlcomposer.Logging.setLog(log);
        com.citesa.intelcomp.Logging.setLog(log);
    }
}
