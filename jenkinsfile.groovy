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
	withCredentials([[$class: 'AmazonWebServicesCredentialsBinding', accessKeyVariable: 'AWS_ACCESS_KEY_ID', credentialsId: 'CFNBot', secretKeyVariable: 'AWS_SECRET_ACCESS_KEY']]) {
            sh """
            export PATH=$PATH:$HOME/.local/bin
            /tmp/apache-maven-3.6.0/bin/mvn -fn verify -DresourceBucket=jenkins-itstack-artifacts
            """
        }
	junit allowEmptyResults: true, testResults: 'fds-it/target/surefire-reports/*.xml'
    }
}
