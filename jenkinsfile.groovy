def repository = "streaming-data-platform"
def organization = "provectus"
node("JenkinsOnDemand") {

    properties([
            // We must disable concurent builds to eliminate the error
            // with only 5 kinesis streams simultaneously
            disableConcurrentBuilds()
    ])

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
      	withCredentials([usernamePassword(credentialsId: 'artifactory', passwordVariable: 'CI_ARTIFACTORY_USER_PASSWORD', usernameVariable: 'CI_ARTIFACTORY_USER_NAME')]) {
            sh "/tmp/apache-maven-3.6.0/bin/mvn clean package"
        }
    }
    stage("Test") {
	withCredentials([[$class: 'AmazonWebServicesCredentialsBinding', accessKeyVariable: 'AWS_ACCESS_KEY_ID', credentialsId: 'CFNBot', secretKeyVariable: 'AWS_SECRET_ACCESS_KEY']]) {
            withCredentials([usernamePassword(credentialsId: 'artifactory', passwordVariable: 'CI_ARTIFACTORY_USER_PASSWORD', usernameVariable: 'CI_ARTIFACTORY_USER_NAME')]) {
                sh """
                export PATH=\$PATH:\$HOME/.local/bin
                BUCKET=jenkins-`cat /dev/urandom | tr -dc 'a-z0-9' | fold -w 32 | head -n 1`
                /tmp/apache-maven-3.6.0/bin/mvn -fn clean package verify -DresourceBucket=\${BUCKET}
                (aws s3 rb s3://\${BUCKET} --force || exit 0)
                """
            }
        }
	junit allowEmptyResults: true, testResults: 'fds-it/target/surefire-reports/*.xml'
    }
}
