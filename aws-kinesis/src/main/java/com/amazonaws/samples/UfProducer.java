package com.amazonaws.samples;

import java.nio.ByteBuffer;

import com.alibaba.fastjson.JSON;
import com.amazonaws.AmazonClientException;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.services.kinesis.AmazonKinesis;
import com.amazonaws.services.kinesis.AmazonKinesisClientBuilder;
import com.amazonaws.services.kinesis.model.PutRecordRequest;
import com.amazonaws.services.kinesis.model.PutRecordResult;

public class UfProducer {
	
	private static AmazonKinesis kinesis;
	
	private static void init() throws Exception {
        /*
         * The ProfileCredentialsProvider will return your [default]
         * credential profile by reading from the credentials file located at
         * (C:\\Users\\liuwei1\\.aws\\credentials).
         */
        ProfileCredentialsProvider credentialsProvider = new ProfileCredentialsProvider();
        try {
            credentialsProvider.getCredentials();
        } catch (Exception e) {
            throw new AmazonClientException(
                    "Cannot load the credentials from the credential profiles file. " +
                    "Please make sure that your credentials file is at the correct " +
                    "location (C:\\Users\\liuwei1\\.aws\\credentials), and is in valid format.",
                    e);
        }

        kinesis = AmazonKinesisClientBuilder.standard()
            .withCredentials(credentialsProvider)
            .withRegion("us-west-2")
            .build();
    }
	
	public static void main(String args[]) throws Exception
	{
		init();
		
		while (true) {
			
			String myStreamName = "uf-flow";
			long createTime = System.currentTimeMillis();

			Uf uf = new Uf();
			
			uf.setId("1");
			uf.setUserName("liuwei");
			uf.setFlowSize(1000L);
			uf.setTimesmap(createTime);
			
			String msg = JSON.toJSONString(uf);

			PutRecordRequest putRecordRequest = new PutRecordRequest();
			putRecordRequest.setStreamName(myStreamName);
			putRecordRequest.setData(ByteBuffer.wrap(msg.getBytes()));
			putRecordRequest.setPartitionKey(String.format("partitionKey-%d", createTime));
			PutRecordResult putRecordResult = kinesis.putRecord(putRecordRequest);

			System.out.printf("Successfully put record, partition key : %s, ShardID : %s, SequenceNumber : %s.\n",
					putRecordRequest.getPartitionKey(), putRecordResult.getShardId(),
					putRecordResult.getSequenceNumber());
		}
	}
	
	
	public static class Uf
	{
		private String id;
		
		private String userName;
		
		private Long flowSize;
		
		private Long timesmap;

		public String getId() {
			return id;
		}

		public void setId(String id) {
			this.id = id;
		}

		public String getUserName() {
			return userName;
		}

		public void setUserName(String userName) {
			this.userName = userName;
		}

		public Long getFlowSize() {
			return flowSize;
		}

		public void setFlowSize(Long flowSize) {
			this.flowSize = flowSize;
		}

		public Long getTimesmap() {
			return timesmap;
		}

		public void setTimesmap(Long timesmap) {
			this.timesmap = timesmap;
		}
	}

}
