BUCKET = a7test_prva

build: 
	mvn clean package
	cp ./target/job-jar-with-dependencies.jar ./job.jar

setupCloud:
	aws s3 mb s3://$(BUCKET)
	aws s3 sync s3://mrclassvitek/a7history/ s3://$(BUCKET)/a7history/
	aws s3 sync s3://mrclassvitek/a7test/ s3://$(BUCKET)/a7test/
	aws s3 sync s3://mrclassvitek/07request/ s3://$(BUCKET)/07request/
	aws s3 cp job.jar s3://$(BUCKET)

run:
	aws emr create-cluster --applications Name=Hadoop Name=Hive Name=Pig Name=Hue --ec2-attributes '{"InstanceProfile":"EMR_EC2_DefaultRole","SubnetId":"subnet-86464df1","EmrManagedSlaveSecurityGroup":"sg-6451f803","EmrManagedMasterSecurityGroup":"sg-6651f801"}' --service-role EMR_DefaultRole --release-label emr-4.4.0 --steps '[{"Args":["com.cs6200.A7.A7","connections","hinput=s3://$(BUCKET)/a7history/","tinput=s3://$(BUCKET)/a7test/","output=s3://$(BUCKET)/output_one/","bucket=s3://$(BUCKET)/"],"Type":"CUSTOM_JAR","ActionOnFailure":"CONTINUE","Jar":"s3://$(BUCKET)/job.jar","Properties":"","Name":"Step 1"},{"Args":["com.cs6200.A7.A7","modeling","s3://$(BUCKET)/user/yarn/hmodel/","s3://$(BUCKET)/"],"Type":"CUSTOM_JAR","ActionOnFailure":"CONTINUE","Jar":"s3://$(BUCKET)/job.jar","Properties":"","Name":"Step 2"},{"Args":["com.cs6200.A7.A7","prediction","s3://$(BUCKET)/user/yarn/tmodel/","s3://$(BUCKET)/07request/","s3://$(BUCKET)/"],"Type":"CUSTOM_JAR","ActionOnFailure":"CONTINUE","Jar":"s3://$(BUCKET)/job.jar","Properties":"","Name":"Step 3"}]' --name 'My cluster' --instance-groups '[{"InstanceCount":5,"InstanceGroupType":"CORE","InstanceType":"m3.xlarge","Name":"Core instance group - 2"},{"InstanceCount":1,"InstanceGroupType":"MASTER","InstanceType":"m3.xlarge","Name":"Master instance group - 1"}]' --auto-terminate --region us-west-2

validate:
	mkdir output
	mkdir a7validate
	aws s3 cp s3://$(BUCKET)/predictions/ output/ --recursive
	aws s3 cp s3://mrclassvitek/a7validate/ a7validate/ --recursive
	java -cp job.jar com.cs6200.A7.A7 validation output a7validate

report:
	Rscript -e "rmarkdown::render('A7.Rmd')"
