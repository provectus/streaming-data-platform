def repository = "squadex-fastdata-solution"
def organization = "provectus"
node("JenkinsOnDemand") {
    stage("Initialize") {
        sh """
        curl -o apache-maven.tar.gz http://archive.apache.org/dist/maven/maven-3/3.6.0/binaries/apache-maven-3.6.0-bin.tar.gz
        tar xzvf apache-maven.tar.gz -C /tmp/
        pip install awscli --upgrade --user
        """
    }
    stage("Checkout") {
        autoCheckout(repository, organization)
    }
    stage("Build") {
        sh "/tmp/apache-maven-3.6.0/bin/mvn clean package"
    }
    stage("Test") {
        sh """
        aws cloudformation package --template-file fds-template.yaml --s3-bucket fds-lambda-java-${env.COMMIT_ID} --output-template-file fds.yaml
        /tmp/apache-maven-3.6.0/bin/mvn verify
        """
    }
}