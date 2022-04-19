package io.fixprotocol.orchestra.avro.v1.fixlatest.example;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import io.fixprotocol.orchestra.avro.v1.fixlatest.messages.BidCompRspGrpItem;
import io.fixprotocol.orchestra.avro.v1.fixlatest.messages.BidResponse;

class BidResponseExampleTest {

    String testFileName = "bid-responses.avro";
    File testFile = new File(testFileName);
	
	@BeforeEach 
	public void before(){
	}

	@AfterEach
	public void after() {// throws IOException{
		if (this.testFile.exists()) {
			this.testFile.delete();
		}
	}
	
	@Test
	void test() throws IOException {
		BidCompRspGrpItem[] bidCompRspGrpItems = new BidCompRspGrpItem[2];
		bidCompRspGrpItems[0] = BidResponseExample.createBidCompRspGrpItem(12.10f, "GBP");
		bidCompRspGrpItems[1] = BidResponseExample.createBidCompRspGrpItem(11.11f, "EUR");
		
		String sendingTime0 = "20220311-15:15:15";
		String sendingTime1 = "20220312-16:16:16";
		String targetCompID = "target";
		String senderCompID = "sender";
		List<BidResponse> bidResponses = new ArrayList<BidResponse>();
		bidResponses.add(BidResponseExample.createBidResponse(senderCompID, targetCompID, sendingTime0, bidCompRspGrpItems));
		bidResponses.add(BidResponseExample.createBidResponse(senderCompID, targetCompID, sendingTime1, bidCompRspGrpItems));
		BidResponseExample.writeBidResponses(bidResponses, testFile);
		List<BidResponse> readBidResponses = BidResponseExample.readBidResponses(testFile);
		assertEquals(bidResponses.get(0), readBidResponses.get(0));
		assertEquals(bidResponses.get(1), readBidResponses.get(1));
	}

}
