AWS_SECRET_KEY = 
AWS_ACCESS_ID = 
INPUT = /home/raiden/all
OUTPUT = /
INPUT_LMR = input
OUTPUT_LMRMD = outputmd
OUTPUT_LMRFM = outputfm
OUTPUT_LMRMN = outputmn
JAR_PATH = s3://chinmayeevalues/job.jar
FS_ROOT = s3://chinmayeevalues/
INPUT_CMR = s3://chinmayeevalues/input
OUTPUT_CMRMD = s3://chinmayeevalues/outputmd
OUTPUT_CMRFM = s3://chinmayeevalues/outputfm
OUTPUT_CMRMN = s3://chinmayeevalues/outputmn

build: 
	mvn clean install
	cp ./target/job-jar-with-dependencies.jar ./job.jar

run:
	java -cp uber-perf-0.0.1.jar  neu.perf.App -num=1  -jar=job.jar  -kind=plain  -main=SingleThreadSol -arguments="-md" -input=$(INPUT)  -output=$(OUTPUT)  -results=results.csv -name=i-median 
	java -cp uber-perf-0.0.1.jar  neu.perf.App -num=1  -jar=job.jar  -kind=plain  -main=SingleThreadSol -arguments="-fm" -input=$(INPUT)  -output=$(OUTPUT)  -results=results.csv -name=i-fastmed 
	java -cp uber-perf-0.0.1.jar  neu.perf.App -num=1  -jar=job.jar  -kind=plain  -main=SingleThreadSol -arguments="-mn" -input=$(INPUT)  -output=$(OUTPUT)  -results=results.csv -name=i-mean 
	java -cp uber-perf-0.0.1.jar  neu.perf.App -num=1  -jar=job.jar  -kind=plain  -main=MultiThreadSol -arguments="-md" -input=$(INPUT)  -output=$(OUTPUT)  -results=results.csv -name=ii-median 
	java -cp uber-perf-0.0.1.jar  neu.perf.App -num=1  -jar=job.jar  -kind=plain  -main=MultiThreadSol -arguments="-fm" -input=$(INPUT)  -output=$(OUTPUT)  -results=results.csv -name=ii-fastmed 
	java -cp uber-perf-0.0.1.jar  neu.perf.App -num=1  -jar=job.jar  -kind=plain  -main=MultiThreadSol -arguments="-mn" -input=$(INPUT)  -output=$(OUTPUT)  -results=results.csv -name=ii-mean 
	java -cp uber-perf-0.0.1.jar  neu.perf.App -num=1  -jar=job.jar  -kind=local  -main=A3 -arguments="-md" -input=$(INPUT_LMR)  -output=$(OUTPUT_LMRMD)  -results=results.csv -name=iii-median 
	java -cp uber-perf-0.0.1.jar  neu.perf.App -num=1  -jar=job.jar  -kind=local  -main=A3 -arguments="-fm" -input=$(INPUT_LMR)  -output=$(OUTPUT_LMRFM)  -results=results.csv -name=iii-fastmed 
	java -cp uber-perf-0.0.1.jar  neu.perf.App -num=1  -jar=job.jar  -kind=local  -main=A3 -arguments="-mn" -input=$(INPUT_LMR)  -output=$(OUTPUT_LMRMN)  -results=results.csv -name=iii-mean 
	java -cp uber-perf-0.0.1.jar  neu.perf.App -num=1  -jar=$(JAR_PATH)  -kind=cloud  -main=A3 -arguments="-md" -fsroot=$(FS_ROOT) -awskeyid=$(AWS_ACCESS_ID) -awskey=$(AWS_SECRET_KEY) -results=results.csv -input=$(INPUT_CMR) -output=$(OUTPUT_CMRMD) -name=iv-median 
	java -cp uber-perf-0.0.1.jar  neu.perf.App -num=1  -jar=$(JAR_PATH)  -kind=cloud  -main=A3 -arguments="-fm" -fsroot=$(FS_ROOT) -awskeyid=$(AWS_ACCESS_ID) -awskey=$(AWS_SECRET_KEY) -results=results.csv -input=$(INPUT_CMR) -output=$(OUTPUT_CMRFM) -name=iv-fastmed 
	java -cp uber-perf-0.0.1.jar  neu.perf.App -num=1  -jar=$(JAR_PATH)  -kind=cloud  -main=A3 -arguments="-mn" -fsroot=$(FS_ROOT) -awskeyid=$(AWS_ACCESS_ID) -awskey=$(AWS_SECRET_KEY) -results=results.csv -input=$(INPUT_CMR) -output=$(OUTPUT_CMRMN) -name=iv-mean 

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
	Rscript -e "rmarkdown::render('A3.Rmd')"
