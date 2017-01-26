AWS_SECRET_KEY = 
AWS_ACCESS_ID = 
INPUT = input
PINPUT = all
BUCKET = 
OUTPUT = output


build: 
	mvn clean install
	cp ./target/job-jar-with-dependencies.jar ./job.jar

upload:
	aws s3 cp job.jar s3://$(BUCKET)

run:
	aws s3 rm s3://${BUCKET}/${OUTPUT} --recursive
	java -cp uber-perf-0.0.1.jar  neu.perf.App -num=1  -jar=s3://${BUCKET}/job.jar  -kind=cloud  -main=A5 -arguments="" -fsroot=s3://$(BUCKET)/ -awskeyid=$(AWS_ACCESS_ID) -awskey=$(AWS_SECRET_KEY) -results=results.csv -input=s3://$(BUCKET)/$(INPUT) -output=s3://$(BUCKET)/$(OUTPUT) -name=conn
	aws s3 sync s3://$(BUCKET)/$(OUTPUT) $(OUTPUT)
	Rscript -e "rmarkdown::render('A5.Rmd')"

pseudo:
	-hadoop fs -rm -r output
	hadoop jar job.jar A5 input=$(PINPUT) output=$(OUTPUT)

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

report:
	Rscript -e "rmarkdown::render('A5.Rmd')"
