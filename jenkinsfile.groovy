def repository = 'squadex-fastdata-solution'
def organization = 'provectus'
node("JenkinsOnDemand") {
    stage("Checkout") {
        autoCheckout(repository, organization)
    }
    stage("Build") {
        sh 'mvn clean package'
    }
    stage("Test") {
        echo 'mvn verify'
    }
}