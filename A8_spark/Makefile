BUCKET = chintanpathak
REGION = us-west-2
KEY_NAME = chintan
SPARK_PATH = /home/raiden/spark-1.6.1
INPUT = /home/raiden/input

build: 
	mvn clean package
	cp ./target/cs6270.A8-0.0.1-SNAPSHOT-jar-with-dependencies.jar ./job.jar
	chmod 777 waitTillCompletion.sh


pseudo:
	-rm -r output_1
	-rm -r output_200
	$(SPARK_PATH)/bin/spark-submit --class com.cs6270.A8.A8 --master local[4] job.jar input=$(INPUT) output=output mode=pseudo
	cat output_1/part-* > output_1/merged_1
	cat output_200/part-* > output_200/merged_200
	make report
	

setupCloud:
	aws s3 mb s3://$(BUCKET)

upload:
	aws s3 cp job.jar s3://$(BUCKET)

run:
	aws s3 rm s3://$(BUCKET)/output_1/ --recursive
	aws s3 rm s3://$(BUCKET)/output_200/ --recursive
	aws emr create-cluster --applications Name=Hadoop Name=Spark --ec2-attributes '{"KeyName":"$(KEY_NAME)","InstanceProfile":"EMR_EC2_DefaultRole"}' --service-role EMR_DefaultRole --release-label emr-4.4.0 --steps '[{"Args":["spark-submit","--deploy-mode","cluster","--class","com.cs6270.A8.A8","--master","yarn","s3://$(BUCKET)/job.jar","input=s3://mrclassvitek/data","output=s3://$(BUCKET)/output","mode=cloud"],"Type":"CUSTOM_JAR","ActionOnFailure":"CONTINUE","Jar":"command-runner.jar","Properties":"","Name":"Spark application"}]' --name 'A8' --instance-groups '[{"InstanceCount":5,"InstanceGroupType":"CORE","InstanceType":"m3.xlarge","Name":"Core instance group - 2"},{"InstanceCount":1,"InstanceGroupType":"MASTER","InstanceType":"m3.xlarge","Name":"Master instance group - 1"}]' --auto-terminate --region $(REGION) --log-uri s3://${BUCKET}/logs	

	sh waitTillCompletion.sh $(BUCKET)
	make downloadOutput
	make report

downloadOutput:
	-rm -r output_1
	-rm -r output_200
	-mkdir output_1
	-mkdir output_200
	aws s3 cp s3://$(BUCKET)/output_1/ output_1/ --recursive
	aws s3 cp s3://$(BUCKET)/output_200/ output_200/ --recursive
	cat output_1/part-* > output_1/merged_1
	cat output_200/part-* > output_200/merged_200

report:
	Rscript -e "rmarkdown::render('A8.Rmd')"
