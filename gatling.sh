#! /bin/bash
API=$1
docker run -it --rm -e JAVA_OPTS="-Dduration=60 -DbaseUrl=https://$API.execute-api.us-west-2.amazonaws.com -Dgatling.core.simulationClass=basic.ApiPerformanceTest" -v  `pwd`/gatling:/opt/gatling/user-files -v `pwd`/gatling/results:/opt/gatling/results -v `pwd`/gatling/conf:/opt/gatling/conf denvazh/gatling