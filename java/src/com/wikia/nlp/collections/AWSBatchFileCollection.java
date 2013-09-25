package com.wikia.nlp.collections;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.*;

import org.apache.commons.io.IOUtils;

import org.apache.commons.compress.archivers.ArchiveException;
import org.apache.commons.compress.archivers.ArchiveStreamFactory;
import org.apache.commons.compress.archivers.tar.TarArchiveEntry;
import org.apache.commons.compress.archivers.tar.TarArchiveInputStream;
import org.apache.commons.compress.compressors.gzip.GzipCompressorInputStream;

import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.ObjectListing;

/**
 * Responsible for iterating through batches of files from S3.
 * This means polling, locking, downloading, untarring, and iterating.
 * Yikes :-\
 */

public class AWSBatchFileCollection extends AbstractCollection<File> implements Iterator<File> {

	private static boolean SAFE = false;
	
    protected AmazonS3Client client;
    
    protected List<File> fileList;
    
    protected String currKey;
    
    protected int fileListCursor = 1;
    
    public AWSBatchFileCollection( AmazonS3Client client ) {
        this.client = client;
    }
    
    /**
     * Driver method
     * @param args
     * @throws IOException
     */
    public static void main(String[] args) throws IOException {
    	SAFE = true;
    	AWSBatchFileCollection coll = new AWSBatchFileCollection(
    										new AmazonS3Client(
    												new BasicAWSCredentials(
    														"AKIAJ6W4PSWS2FU7IK5A", 
    														"ZiIS6ujt+4c8I7IfmwyHccOgCkP9AFPLJsXICnc+"
    														)
    												)
    										);
    	System.out.println("here");
    	for (File f: coll) {
    		System.out.println(f);
    	}
    	System.out.println("done");
    }
    
    /**
     * Allows us to use this collection as an iterator
     * @return Iterator<File>
     */
    public Iterator<File> iterator() {
    	if (this.fileList == null) {
    		this.getFileList();
    	}
    	
    	return this;
    }
    
    public void remove() {}
    
    public List<File> getFileList() {
    	if ((this.fileList == null) || (this.fileList.size() == this.fileListCursor - 1)) {
    		try {
    			// @TODO: delete old key 
    			this.fileList = this.getExtractedFileListFromAws();
    			this.fileListCursor = 1;
    		} catch ( InterruptedException e ) { }
    	}
    	return this.fileList;
    }
    
    /**
     * Should basically always be true, since we wait until a new file list shows up
     */
    public boolean hasNext()  {
    	return (this.getFileList().size() > this.fileListCursor - 1);
    }
    
    public File next() {
    	return this.getFileList().get(this.fileListCursor++ - 1);
    }
    
    /**
     * Provides the length of the current file list
     * Only use this to check if empty.
     * @return integer
     */
    public int size() {
    	return this.getFileList().size();
    }

    /**
     * Pulls down a .tgz file and extracts it, providing a file list to iterate over.
     * @return List<File> a list of files that have been extracted from an AWS event file
     * @throws InterruptedException
     */
    protected List<File> getExtractedFileListFromAws() throws InterruptedException {
        ObjectListing listing = this.client.listObjects("nlp-data", "text_events");
        List<S3ObjectSummary> objectSummaries = listing.getObjectSummaries();
        while (true) {
	        for (int i = 0; i < objectSummaries.size(); i++) {
	        	S3ObjectSummary objectSummary = objectSummaries.get(i);
	        	try {
	        		if (this.currKey != null && !SAFE) {
	        			this.client.deleteObject("nlp-data", this.currKey);
	        		}
	        		String oldKey = objectSummary.getKey();
	        		if (! oldKey.endsWith(".tgz") ) {
	        			continue;
	        		}
	        		String newKey = oldKey.replace("text_events/", "parser_processing/");
	        		this.client.copyObject("nlp-data", oldKey, "nlp-data", newKey);
	        		this.client.deleteObject("nlp-data", oldKey);
	        		if (SAFE) {
	        			this.client.copyObject("nlp-data", newKey, "nlp-data", oldKey);
	        		}
	        		this.currKey = newKey;
	        		return this.extractFileListFromAwsFromObject(this.client.getObject("nlp-data", newKey), this.primeDir("/tmp/text"));
	        	} catch ( Exception e ) {
	        		System.out.println(e);
	        		// probably got taken, iterate
	        	}
	        }
	        Thread.sleep(60); // no files available, so sleep for a bit (todo: maybe die?)
        }
    }
    
    /** Untar an input file into an output file. 
     * Stolen from http://stackoverflow.com/questions/315618/how-do-i-extract-a-tar-file-in-java
     * Because I'm a REALLY GOOD JAVA PROGRAMMAR
     * 
     * The output file is created in the output folder, having the same name
     * as the input file, minus the '.tar' extension. 
     * 
     * @param inputFile     the input .tar file
     * @param outputDir     the output directory file. 
     * @throws IOException 
     * @throws FileNotFoundException
     *  
     * @return  The {@link List} of {@link File}s with the untared content.
     * @throws ArchiveException 
     */
    protected List<File> extractFileListFromAwsFromObject(final S3Object s3object, final File outputDir) throws FileNotFoundException, IOException, ArchiveException {

        //LOG.info(String.format("Untaring %s to dir %s.", inputFile.getAbsolutePath(), outputDir.getAbsolutePath()));

    	final List<File> untaredFiles = new LinkedList<File>();
    	final GzipCompressorInputStream gzIn = new GzipCompressorInputStream(new BufferedInputStream(s3object.getObjectContent()));
        final TarArchiveInputStream debInputStream = (TarArchiveInputStream) new ArchiveStreamFactory().createArchiveInputStream("tar", gzIn);
        TarArchiveEntry entry = null; 
        while ((entry = (TarArchiveEntry)debInputStream.getNextEntry()) != null) {
            final File outputFile = new File(outputDir, entry.getName());
            if (entry.isDirectory()) {
                //LOG.info(String.format("Attempting to write output directory %s.", outputFile.getAbsolutePath()));
                if (!outputFile.exists()) {
                    //LOG.info(String.format("Attempting to create output directory %s.", outputFile.getAbsolutePath()));
                    if (!outputFile.mkdirs()) {
                        throw new IllegalStateException(String.format("Couldn't create directory %s.", outputFile.getAbsolutePath()));
                    }
                }
            } else {
                //LOG.info(String.format("Creating output file %s.", outputFile.getAbsolutePath()));
                final OutputStream outputFileStream = new FileOutputStream(outputFile);
                IOUtils.copy(debInputStream, outputFileStream);
                outputFileStream.close();
            }
            untaredFiles.add(outputFile);
        }
        debInputStream.close(); 
        return untaredFiles;
    }
    
    /**
     * Creates directory if doesn't exist
     * @param dirName
     * @return File
     */
    protected File primeDir( String dirName ) {
    	File theDir = new File( dirName );
    	if (!theDir.exists()) {
    		theDir.mkdir();
    	}
    	return theDir;
    }
}