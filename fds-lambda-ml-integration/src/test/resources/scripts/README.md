# Helpers scripts for the testing purpose #

## it
 
Templates for integration testing. These scripts 
is under construction and have a better replacement with class
AwsFdsTestIt.
     
## sam 

Helper scripts for lambdas local debugging with IDE.

### How to do lambdas local debugging 

I will describe how to debug a lambda function which handles 
an event such as `s3 put` one. 

First, write you lambda and configure it with the cloudformation 
template. In our case, it must be the fds-template.yaml in the 
project root.

Next, generate event what you want to send while you will have 
debugging the lambda. It may be achieved by running this command:

~~~~
sam local generate-event s3 put > event_raw.json
~~~~

And edit `event_raw.json` as you want. Change the bucket name 
and the key name. Also, change the region where this bucket is located. 

Then in the one terminal windows run script `start-lambdas` and
in the different one `invoke-lambda` with the first parameter is 
previously generated the event file `event_raw.json` and
the second parameter is the lambdas name.

For example

~~~~
./invoke_lambda event_raw.json JsonToParquetLambda
~~~~

Finally, create open the project in your favorite IDE and create
`Remote` configuration. Usually, all parameters can be left as is.

Check that remote host is `localhost` and remote port is `5005`.

Set up a breakpoint in the project code and run just created 
configuration. 

Voila, now you can debug your code with your favorite debugger.    

