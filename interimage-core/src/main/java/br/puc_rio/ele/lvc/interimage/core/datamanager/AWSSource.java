/*Copyright 2014 Computer Vision Lab

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.*/

package br.puc_rio.ele.lvc.interimage.core.datamanager;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileInputStream;

import br.puc_rio.ele.lvc.interimage.data.Image;

import com.amazonaws.ClientConfiguration;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.CannedAccessControlList;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.services.s3.transfer.MultipleFileUpload;
import com.amazonaws.services.s3.transfer.ObjectMetadataProvider;
import com.amazonaws.services.s3.transfer.TransferManager;
import com.amazonaws.services.s3.transfer.TransferManagerConfiguration;
import com.amazonaws.services.s3.transfer.Upload;

/**
 * A Source class that communicates with Amazon S3. 
 * @author Rodrigo Ferreira
 */
@SuppressWarnings("unused")
public class AWSSource implements Source {

	private String _accessKey;	
	private String _secretKey;
	private String _bucket;
	private TransferManager _manager;
	
	public AWSSource(String accessKey, String secretKey, String bucket) {
		_accessKey = accessKey;
		_secretKey = secretKey;
		_bucket = bucket;
		
		AWSCredentials credentials = new BasicAWSCredentials(_accessKey, _secretKey);
		
		ClientConfiguration conf = new ClientConfiguration();
		
		conf.setConnectionTimeout(0);
		conf.setSocketTimeout(0);
		
		AmazonS3 conn = new AmazonS3Client(credentials);
		conn.setEndpoint("https://s3.amazonaws.com");
		
		_manager = new TransferManager(conn);
				
	}
	
	public void put(String from, String to, Resource resource) {

		try {
					
			File file = new File(from);
							
			PutObjectRequest putObjectRequest = new PutObjectRequest(_bucket, to, file);
			
			if (resource instanceof SplittableResource) {
				SplittableResource rsrc = (SplittableResource)resource;
				if (rsrc.getType() == SplittableResource.IMAGE) {
					putObjectRequest.withCannedAcl(CannedAccessControlList.PublicRead); // public for all
				}
			} else if (resource instanceof DefaultResource) {
				DefaultResource rsrc = (DefaultResource)resource;
				if (rsrc.getType() == DefaultResource.TILE) {
					putObjectRequest.withCannedAcl(CannedAccessControlList.PublicRead); // public for all
				} else if (rsrc.getType() == DefaultResource.FUZZY_SET) {
					putObjectRequest.withCannedAcl(CannedAccessControlList.PublicRead); // public for all
				} else if (rsrc.getType() == DefaultResource.SHAPE) {
					putObjectRequest.withCannedAcl(CannedAccessControlList.PublicRead); // public for all
				}
			}
		
			Upload upload = _manager.upload(putObjectRequest);
			
			upload.waitForCompletion();
			
			System.out.println("AWSSource: Uploaded file - " + to);
			
		} catch (Exception e) {
			System.err.println("Source put failed: " + e.getMessage());			
		}
		
	}
	
	public void multiplePut(File dir, String key) {
		try {			
			MultipleFileUpload upload = _manager.uploadDirectory(_bucket, key, dir, false);			
			upload.waitForCompletion();			
		} catch (Exception e) {
			e.printStackTrace();			
		}
		
		System.out.println("AWSSource: Uploaded directory - " + dir.toString());
		
	}
	
	public void makePublic(String key) {
		//_manager.getAmazonS3Client().setObjectAcl(_bucket, key, CannedAccessControlList.PublicRead);
		System.out.println("AWSSource: Made public - " + key);
	}
	
	public String getSpecificURL() {
		return "s3n://" + _bucket + "/";
	}
	
	public String getURL() {
		return "https://s3.amazonaws.com/" + _bucket + "/";
	}
		
	public void close() {
		_manager.shutdownNow();
	}
	
}
