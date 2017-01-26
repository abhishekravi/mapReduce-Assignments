import java.io.InputStream;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FSDataInputStream;

import com.amazonaws.AmazonClientException;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.transfer.TransferManager;
import com.amazonaws.services.s3.transfer.Upload;

/**
 * AWS helper class to upload and download s3 files.
 * @author Abhishek Ravi Chandran
 *
 */
public class AWSHelperClass {
	
	AmazonS3 s3;
	String bucket;
	private Log LOGGER = LogFactory.getLog(AWSHelperClass.class);
	
	/**
	 * constructor to create s3 connections.
	 * @param accessid
	 * id
	 * @param secretKey
	 * secret key
	 * @param bucket
	 * bucket
	 */
	AWSHelperClass(String accessid, String secretKey, String bucket){
		this.bucket = bucket;
		s3 = new AmazonS3Client(new BasicAWSCredentials(accessid, secretKey));
	}
	
	/**
	 * method to upload file to s3.
	 * @param filename
	 * filename to upload
	 * @param file
	 * file as data stream
	 * @param folder
	 * folder to upload to
	 */
	public void upload(String filename, FSDataInputStream file,String folder) {
		LOGGER.info("uploading " + filename + "to s3");
		TransferManager transferManager  = new TransferManager(s3);
		ObjectMetadata objectMetadata =  new ObjectMetadata();
		Upload upload =  transferManager.upload(bucket, folder+"/"+filename, file, objectMetadata);
		try {upload.waitForCompletion();} 
		catch (AmazonClientException | InterruptedException e) {
			LOGGER.error(e.getMessage());
		}
	}
	
	/**
	 * mehtod to download file from s3.
	 * @param filename
	 * file to download
	 * @param from
	 * from folder
	 * @return
	 * file as stream
	 */
	public InputStream download(String filename, String from){
		 S3Object s3object = s3.getObject(new GetObjectRequest(
         		bucket, from+filename));
		 InputStream input = s3object.getObjectContent();
		 return input;
	}
}