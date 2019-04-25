package com.provectus.fds.ml;

import com.provectus.fds.ml.utils.IntegrationModuleHelper;

import java.util.Map;

import static com.provectus.fds.ml.processor.CsvRecordProcessor.*;

/**
 * This sample app allows you execute an integration check...
 */
public class AthenaApp {
    public static void main(String[] args) throws Exception {
        Map<String, String> statistic = new JobDataGenerator().generateTrainingData(
                new IntegrationModuleHelper(), true
        );

        dumpStatistic(statistic);
    }

    public static void dumpStatistic(Map<String, String> statistic) {
        System.out.println(statistic.get(TOTAL_TRAIN_RECORDS_PROCESSED));
        System.out.println(statistic.get(TOTAL_VERIFY_RECORDS_PROCESSED));
        System.out.println(statistic.get("train_key"));
        System.out.println(statistic.get("train_bucket"));
        System.out.println(statistic.get("validation_key"));
        System.out.println(statistic.get("validation_bucket"));
    }
}
