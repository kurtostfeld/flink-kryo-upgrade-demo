package demo.app;

import org.apache.flink.client.cli.CliFrontend;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Stop {
    public static void main(String[] args) {
        final Logger logger = LoggerFactory.getLogger(Run.class);
        try {
            try {
                logger.info("Starting up.");

                String jobIdString = "e9759a0394a0a856d1dfe1a84109ca78";
                CliFrontend.main(new String[] {
                        "stop", "--type", "native",
                        "--savepointPath", DemoFilePaths.KRYO_NEW_SAVEPOINT_DIR,
                        jobIdString
                });
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
