AWS_SECRET_KEY = 
AWS_ACCESS_ID = 
INPUT = s3://mrclassvitek/data
BUCKET = 
OUTPUT1 = output1
OUTPUT200 = output200


build: 
	mvn clean install
	cp ./target/job-jar-with-dependencies.jar ./job.jar

upload:
	aws s3 cp job.jar s3://$(BUCKET)

run:
	aws s3 rm s3://${BUCKET}/${OUTPUT1} --recursive
	aws s3 rm s3://${BUCKET}/${OUTPUT1}_final --recursive
	java -cp uber-perf-0.0.1.jar  neu.perf.App -num=1  -jar=s3://${BUCKET}/job.jar  -kind=cloud  -main=A4 -arguments="-time=1 $(BUCKET) $(AWS_SECRET_KEY) $(AWS_ACCESS_ID)" -fsroot=s3://$(BUCKET)/ -awskeyid=$(AWS_ACCESS_ID) -awskey=$(AWS_SECRET_KEY) -results=results.csv -input=$(INPUT) -output=s3://$(BUCKET)/$(OUTPUT1) -name=time-1
	aws s3 sync s3://$(BUCKET)/$(OUTPUT1)_final $(OUTPUT1)_final
	aws s3 rm s3://${BUCKET}/${OUTPUT200} --recursive
	aws s3 rm s3://${BUCKET}/${OUTPUT200}_final --recursive
	java -cp uber-perf-0.0.1.jar  neu.perf.App -num=1  -jar=s3://${BUCKET}/job.jar  -kind=cloud  -main=A4 -arguments="-time=200 $(BUCKET) $(AWS_SECRET_KEY) $(AWS_ACCESS_ID)" -fsroot=s3://$(BUCKET)/ -awskeyid=$(AWS_ACCESS_ID) -awskey=$(AWS_SECRET_KEY) -results=results.csv -input=$(INPUT) -output=s3://$(BUCKET)/$(OUTPUT200) -name=time-200
	aws s3 sync s3://$(BUCKET)/$(OUTPUT200)_final $(OUTPUT200)_final
	Rscript -e "rmarkdown::render('A4.Rmd')"

runn1:
	aws s3 rm s3://${BUCKET}/${OUTPUT1} --recursive
	aws s3 rm s3://${BUCKET}/${OUTPUT1}_final --recursive
	java -cp uber-perf-0.0.1.jar  neu.perf.App -num=1  -jar=s3://${BUCKET}/job.jar  -kind=cloud  -main=A4 -arguments="-time=1 $(BUCKET) $(AWS_SECRET_KEY) $(AWS_ACCESS_ID)" -fsroot=s3://$(BUCKET)/ -awskeyid=$(AWS_ACCESS_ID) -awskey=$(AWS_SECRET_KEY) -results=results.csv -input=$(INPUT) -output=s3://$(BUCKET)/$(OUTPUT1) -name=time-1
	aws s3 sync s3://$(BUCKET)/$(OUTPUT1)_final $(OUTPUT1)_final

runn200:
	aws s3 rm s3://${BUCKET}/${OUTPUT200} --recursive
	aws s3 rm s3://${BUCKET}/${OUTPUT200}_final --recursive
	java -cp uber-perf-0.0.1.jar  neu.perf.App -num=1  -jar=s3://${BUCKET}/job.jar  -kind=cloud  -main=A4 -arguments="-time=200 $(BUCKET) $(AWS_SECRET_KEY) $(AWS_ACCESS_ID)" -fsroot=s3://$(BUCKET)/ -awskeyid=$(AWS_ACCESS_ID) -awskey=$(AWS_SECRET_KEY) -results=results.csv -input=$(INPUT) -output=s3://$(BUCKET)/$(OUTPUT200) -name=time-200
	aws s3 sync s3://$(BUCKET)/$(OUTPUT200)_final $(OUTPUT200)_final

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
	Rscript -e "rmarkdown::render('A4.Rmd')"
