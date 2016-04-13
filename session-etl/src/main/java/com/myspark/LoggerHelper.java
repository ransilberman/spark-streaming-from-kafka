package main.java.com.playtika;

import org.apache.log4j.*;
import org.slf4j.LoggerFactory;

/**
 * Created by rans on 21/03/16.
 */
public class LoggerHelper {
    static Logger log = Logger.getLogger(LoggerHelper.class.getName());
    public static void setStreamingLogLevels() {
        log.info("Setting log level to [WARN] for streaming example." +
                " To override add a custom log4j.properties to the classpath.");
            Logger.getRootLogger().setLevel(Level.WARN);
//        Logger root = (Logger) LoggerFactory.getLogger();
//        root.setLevel(Level.INFO);
    }
}
