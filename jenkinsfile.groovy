def repository = 'squadex-fastdata-solution'
def organization = 'provectus'
node('JenkinsOnDemand') {
    tool type: 'maven'
    stage('Checkout') {
        autoCheckout(repository, organization)
    }
    stage('Build') {
        withMaven {
            sh 'mvn clean package'
        }
    }
    stage('Test') {
        withMaven {
            echo 'mvn verify'
        }
    }
}