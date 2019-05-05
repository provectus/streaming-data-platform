package com.provectus.fds.it;

public interface ItConfig {
    String STACK_NAME_PREFIX = "integration";
    String REGION = "us-west-2";

    /**
     * This bucket is temporary place for the template file. It is
     * limitation of cloudformation template with limit exceed more
     * than 51200 bytes.
     *
     * @see <a href="https://docs.aws.amazon.com/en_us/AWSCloudFormation/latest/UserGuide/cloudformation-limits.html">
     *     AWS CloudFormation Limits</a>
     */
    String TEMPLATE_BUCKET = "fds-lambda-java";

    String BID_TYPE = "bid";
    String IMP_TYPE = "impression";
    String CLICK_TYPE = "click";
    String USER_AGENT = "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_12_6) AppleWebKit/603.3.8 (KHTML, like Gecko) Version/10.1.2 Safari/603.3.8";
}
