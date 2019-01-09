def repository = 'squadex-fastdata-solution'

def buildFunction = {
    echo "mvn clean package"
}

pipelineCommon(
        repository,
        false,
        [],
        {},
        buildFunction,
        buildFunction,
        buildFunction,
        null,
        "",
        "",
        {},
        null
)
