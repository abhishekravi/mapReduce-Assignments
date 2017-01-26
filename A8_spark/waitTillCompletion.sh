count=`aws s3 ls s3://$1/output_1/ | grep "_SUCCESS" | wc -l`

while [ $count -ne 1 ]
do
        echo "Waiting for the job to finish"
        sleep 15;
	count=`aws s3 ls s3://$1/output_1/ | grep "_SUCCESS" | wc -l`
done

count=`aws s3 ls s3://$1/output_200/ | grep "_SUCCESS" | wc -l`

while [ $count -ne 1 ]
do
        echo "Waiting for the job to finish"
        sleep 15;
	count=`aws s3 ls s3://$1/output_200/ | grep "_SUCCESS" | wc -l`
done
