package io.fixprotocol.orchestra.avro.v1.fixlatest.example;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import io.fixprotocol.orchestra.avro.v1.fixlatest.groups.BidCompRspGrpItem;
import io.fixprotocol.orchestra.avro.v1.fixlatest.messages.BidResponse;

class BidResponseExampleNormalisedTest {

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
		bidCompRspGrpItems[0] = BidResponseExampleNormalised.createBidCompRspGrpItem("12.10", "GBP");
		bidCompRspGrpItems[1] = BidResponseExampleNormalised.createBidCompRspGrpItem("11.11", "EUR");
		
		String sendingTime0 = "20220311-15:15:15";
		String sendingTime1 = "20220312-16:16:16";
		String targetCompID = "target";
		String senderCompID = "sender";
		List<BidResponse> bidResponses = new ArrayList<BidResponse>();
		bidResponses.add(BidResponseExampleNormalised.createBidResponse(senderCompID, targetCompID, sendingTime0, bidCompRspGrpItems));
		bidResponses.add(BidResponseExampleNormalised.createBidResponse(senderCompID, targetCompID, sendingTime1, bidCompRspGrpItems));
		BidResponseExampleNormalised.writeBidResponses(bidResponses, testFile);
		List<BidResponse> readBidResponses = BidResponseExampleNormalised.readBidResponses(testFile);
		assertEquals(bidResponses.get(0), readBidResponses.get(0));
		assertEquals(bidResponses.get(1), readBidResponses.get(1));
	}

}
