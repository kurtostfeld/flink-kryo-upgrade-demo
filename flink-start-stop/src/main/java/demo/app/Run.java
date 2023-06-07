package demo.app;

import org.apache.flink.client.cli.CliFrontend;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Run {
    public static void main(String[] args) {
        final Logger logger = LoggerFactory.getLogger(Run.class);
        try {
            try {
                logger.info("Starting up.");

                CliFrontend.main(new String[] {
                    "run", "-d", "-t", "remote",
                    "-s", DemoFilePaths.KRYO_V2_NATIVE_SAVEPOINT_PATH, DemoFilePaths.DEMO_APP_UPGRADED_JAR_PATH
                });

                logger.info("Finished Flink run.");
            } catch (Exception e) {
                System.out.printf("println top level exception. %s: %s%n",
                        e.getClass().getSimpleName(), e.getMessage());
                logger.error("top level application exception", e);
            }
        } catch (RuntimeException e) {
            System.out.printf("println top level RuntimeException. %s: %s%n",
                    e.getClass().getSimpleName(), e.getMessage());
            logger.error("top level application RuntimeException", e);
        }

        logger.info("exiting...");
    }
}
