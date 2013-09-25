package com.wikia.nlp;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.PrintWriter;
import java.io.UnsupportedEncodingException;

import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.transfer.TransferManager;
import com.amazonaws.services.s3.transfer.Upload;

public class FileTransferer {

	
	public static void main(String args[]) throws FileNotFoundException, UnsupportedEncodingException {
		TransferManager tm = new TransferManager(new AmazonS3Client(
    												new BasicAWSCredentials(
    														"AKIAJ6W4PSWS2FU7IK5A", 
    														"ZiIS6ujt+4c8I7IfmwyHccOgCkP9AFPLJsXICnc+"
    														)
    												)
												 );
		File writtenFile = new File("/tmp/foo.txt");
		PrintWriter writer = new PrintWriter("/tmp/foo.txt", "UTF-8");
		writer.println("The first line");
		writer.println("The second line");
		writer.close();
		Upload ul = tm.upload("nlp-data", "test", writtenFile);
		System.out.println("uploaded");
		System.exit(0);
	}
	
}
