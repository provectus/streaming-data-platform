#!/bin/bash


REGIONS="us-east-1 us-east-2 us-west-2 eu-west-1 eu-central-1"

for region in $REGIONS; do
    service_prefix="sdp-$(cat /dev/urandom | tr -dc 'a-z' | fold -w 7 | head -n 1)"
    echo Processing $region
    export AWS_DEFAULT_REGION=$region

    cat <<EOT > .taskcat.yml
project:
  name: sdp
  regions:
    - $region
  template: ./fds.yaml
tests:
  t1:
    parameters:
        ServicePrefix: $service_prefix
    regions:
      - $region
EOT

    ./stack.sh -p -t -c -r ${AWS_DEFAULT_REGION} -v tcat -b fdp-itstack-${AWS_DEFAULT_REGION}
    taskcat -d test run
done

