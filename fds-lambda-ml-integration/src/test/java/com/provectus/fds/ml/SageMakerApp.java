package com.provectus.fds.ml;

import com.provectus.fds.ml.utils.IntegrationModuleHelper;

import java.io.IOException;

public class SageMakerApp {
    public static void main(String[] args) throws IOException {
        new JobRunner()
                .createJob(
                        new IntegrationModuleHelper(), true,
                        "s3://newfdsb/athena/train_4188522943897605003.csv",
                        "s3://newfdsb/athena/verify_8880021217306585575.csv");
    }
}
