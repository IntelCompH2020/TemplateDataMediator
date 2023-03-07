package com.citesa.intelcomp.sparksqltemplatedatamediator;

import com.citesa.intelcomp.infrahelper.filereader.SimpleFileReader;
import com.citesa.intelcomp.infrahelper.filereader.SimpleFileReaderBase;
import com.citesa.intelcomp.infrahelper.filewriter.SimpleFileWriter;
import com.citesa.intelcomp.infrahelper.filewriter.SimpleFileWriterBase;
import com.citesa.spark.sqlcomposer.CacheMetadata;

import java.io.IOException;
import java.net.URI;
import java.util.List;

public class CacheManager {

    public static void clearCache(String metadataPath, List<String> hashes) {
        try {
            SimpleFileReader fileReader = SimpleFileReaderBase.getFileReader(URI.create(metadataPath));
            List<String> files = fileReader.getAvailableFiles();
            for (String file: files) {
                CacheMetadata cacheMetadata = CacheMetadata.loadMetadata(file);
                if (cacheMetadata != null && !hashes.contains(cacheMetadata.getHash()) && !cacheMetadata.evaluateCache()) {
                    String cachePath = cacheMetadata.getLocation();
                    SimpleFileWriter sfw = SimpleFileWriterBase.getFileWriter(URI.create(file));
                    if (sfw != null) {
                        sfw.delete();
                    }
                    sfw = SimpleFileWriterBase.getFileWriter(URI.create(cachePath));
                    if (sfw != null) {
                        sfw.delete();
                    }
                }
            }
        } catch (IOException e) {
            Logging.getLog().logAndPrintExceptionError(e, "Failed to clear cache");
        }
    }
}
