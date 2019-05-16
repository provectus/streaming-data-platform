package com.provectus.fds.ml;

import java.util.Collections;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

class SagemakerAlgorithmsRegistry {

    // This map intended only for these algorithms
    // 'pca', 'kmeans', 'linear-learner', 'factorization-machines', 'ntm',
    // 'randomcutforest', 'knn', 'object2vec', 'ipinsights'
    //
    // If you want some different algos, then visit
    // https://github.com/aws/sagemaker-python-sdk/blob/master/src/sagemaker/amazon/amazon_estimator.py#L286
    // and implement absent part.
    @java.lang.SuppressWarnings("squid:S3878")
    private Map<String, String> commonContainers = Stream.of(new String[][] {
                    { "us-east-1", "382416733822" },
                    { "us-east-2", "404615174143" },
                    { "us-west-2", "174872318107" },
                    { "eu-west-1", "438346466558" },
                    { "eu-central-1", "664544806723" },
                    { "ap-northeast-1", "351501993468" },
                    { "ap-northeast-2", "835164637446" },
                    { "ap-southeast-2", "712309505854" },
                    { "us-gov-west-1", "226302683700" },
                    { "ap-southeast-1", "475088953585" },
                    { "ap-south-1", "991648021394" },
                    { "ca-central-1", "469771592824" },
                    { "eu-west-2", "644912444149" },
                    { "us-west-1", "632365934929" },
                    { "us-iso-east-1", "490574956308" },
    }).collect(Collectors.collectingAndThen(
            Collectors.toMap(data -> data[0], data -> data[1]),
            Collections::unmodifiableMap));

    public static class RegistryException extends RuntimeException {
        RegistryException(String message) {
            super(message);
        }
    }

    public String getImageUri(String regionName, String algorithm) {
        switch (algorithm) {
            case "pca":
            case "kmeans":
            case "linear-learner":
            case "factorization-machines":
            case "ntm":
            case "randomcutforest":
            case "knn":
            case "object2vec":
            case "ipinsights":
                return getEcrImageUriPrefix(commonContainers.get(regionName), regionName);
            default:
                throw new RegistryException("Algorithm " + algorithm + " is not supported by this class.\n" +
                        "Probably, you must extend this class by yourself!");
        }
    }

    public String getFullImageUri(String regionName, String algorithm) {
        return getImageUri(regionName, algorithm) + "/" + algorithm + ":latest";
    }

    private String getEcrImageUriPrefix(String account, String region) {
        String domain = "amazonaws.com";
        if ("us-iso-east-1".equals(region)) {
            domain = "c2s.ic.gov";
        }
        return String.format("%s.dkr.ecr.%s.%s", account, region, domain);
    }
}
