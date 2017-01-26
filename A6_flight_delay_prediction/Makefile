AWS_SECRET_KEY = 
AWS_ACCESS_ID = 
#historical data input
HINPUT = a6history
#test data input
TINPUT = a6test
#log file path
LOGPATH = logs
#s3 bucket name
BUCKET = pdmrbucket
#output folder name
OUTPUT = output
#hadoop root path
FSPATH = /user/hduser
#validation file path
VPATH = /home/raiden/mrdata/a6validate/

build: 
	mvn clean install
	cp ./target/job-jar-with-dependencies.jar ./job.jar

setup:
	aws s3 rm s3://${BUCKET}/${OUTPUT} --recursive
	aws s3 rm s3://${BUCKET}/${OUTPUT}_final --recursive
	aws s3 rm s3://${BUCKET}/temp --recursive
	aws s3 cp job.jar s3://$(BUCKET)


run:	
	java -cp uber-perf-0.0.1.jar  neu.perf.App -num=1  -jar=s3://${BUCKET}/job.jar  -kind=cloud  -main=A6 -arguments="-tinput=s3://$(BUCKET)/$(TINPUT) awsid=$(AWS_ACCESS_ID) awskey=$(AWS_SECRET_KEY) mode=cloud" -fsroot=s3://$(BUCKET)/ -awskeyid=$(AWS_ACCESS_ID) -awskey=$(AWS_SECRET_KEY) -results=results.csv -input=s3://$(BUCKET)/$(HINPUT) -output=s3://$(BUCKET)/$(OUTPUT) -name=predictcloud
	aws s3 sync s3://$(BUCKET)/$(OUTPUT)_final output_final
	java -cp job.jar Output $(VPATH) ./output_final/
	Rscript -e "rmarkdown::render('A6.Rmd')"

cloud:
	aws emr create-cluster --name "A6" --release-label emr-4.3.0 --instance-groups InstanceGroupType=MASTER,InstanceCount=1,InstanceType=m3.xlarge InstanceGroupType=CORE,InstanceCount=3,InstanceType=m3.xlarge --steps Type=CUSTOM_JAR,Name="job JAR Step",ActionOnFailure=CONTINUE,Jar=s3n://$(BUCKET)/job.jar,MainClass=A6,Args=[hinput=s3n://$(BUCKET)/$(HINPUT),output=s3n://$(BUCKET)/$(OUTPUT),tinput=s3n://$(BUCKET)/$(TINPUT),awsid=$(AWS_ACCESS_ID),awskey=$(AWS_SECRET_KEY),bucket=$(BUCKET),mode=cloud] --auto-terminate --log-uri s3n://$(BUCKET)/$(LOGPATH) --service-role EMR_DefaultRole --ec2-attributes InstanceProfile=EMR_EC2_DefaultRole,AvailabilityZone=us-west-2b --enable-debugging

pseudo:
	-hadoop fs -rm -r $(OUTPUT)
	-hadoop fs -rm -r $(OUTPUT)_final
	-hadoop fs -rm -r histtemp
	-hadoop fs -rm -r testtemp
	-hadoop fs -rm -r final
	hadoop jar job.jar A6 hinput=$(HINPUT) output=$(OUTPUT) tinput=$(TINPUT) awsid=$(AWS_ACCESS_ID) awskey=$(AWS_SECRET_KEY) bucket=$(BUCKET) mode=pseudo

test:
	-hadoop fs -rm -r $(OUTPUT)_final
	-hadoop fs -rm -r testtemp
	hadoop jar job.jar A6 hinput=$(HINPUT) output=$(OUTPUT) tinput=$(TINPUT) awsid=asd awsk=as bucket=$(FSPATH) mode=pseudo


format: 
	hdfs namenode -format

hstart:
	start-dfs.sh
	start-yarn.sh
	mr-jobhistory-daemon.sh start historyserver

hstop:
	mr-jobhistory-daemon.sh stop historyserver 
	stop-yarn.sh
	stop-dfs.sh

graph:
	Rscript -e "rmarkdown::render('A6.Rmd')"

unsafe:
	hdfs dfsadmin -safemode leave

report:	
	aws s3 sync s3://$(BUCKET)/$(OUTPUT)_final output_final
	java -cp job.jar Output $(VPATH) ./output_final/
	Rscript -e "rmarkdown::render('A6.Rmd')"
