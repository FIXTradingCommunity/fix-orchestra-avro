package io.fixprotocol.orchestra.avro.v1.fixlatest.example;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.specific.SpecificDatumWriter;

import io.fixprotocol.orchestra.avro.v1.fixlatest.codesets.MsgTypeCodeSet;
import io.fixprotocol.orchestra.avro.v1.fixlatest.components.CommissionData;
import io.fixprotocol.orchestra.avro.v1.fixlatest.components.StandardHeader;
import io.fixprotocol.orchestra.avro.v1.fixlatest.components.StandardTrailer;
import io.fixprotocol.orchestra.avro.v1.fixlatest.groups.BidCompRspGrp;
import io.fixprotocol.orchestra.avro.v1.fixlatest.groups.BidCompRspGrpItem;
import io.fixprotocol.orchestra.avro.v1.fixlatest.messages.BidResponse;


/**
 * See test classes for invocation of the example
 * This example is very contrived as it uses all the fields and components for a standard FIX Orchestration
 * Avro users are expected to customise the FIX Orchestra Repository from which the schema is generated, 
 * for example fields like body length and checksum, though mandatory in FIX will not add value for many Avro use cases 
 */
public class BidResponseExampleNormalised {
	

	private static final String FIXT_1_1 = "FIXT.1.1";
	
	/**
	 * Create a <code>BidResponse</code>
	 * @param senderCompID
	 * @param targetCompID
	 * @param sendingTime
	 * @param bidCompRspGrpItems
	 * @return
	 */
	public static BidResponse createBidResponse(String senderCompID, String targetCompID, String sendingTime, BidCompRspGrpItem[] bidCompRspGrpItems) {
		// it does not make sense to set body length or checksum for an AVRO message but this example is based on a standard, un-customised orchestration
		StandardHeader standardHeader = StandardHeader.newBuilder().setBeginString(FIXT_1_1).				setBodyLength(0).    
				setMsgType(MsgTypeCodeSet.BID_RESPONSE).
				setSenderCompID(senderCompID).
				setTargetCompID(targetCompID).
				setMsgSeqNum(2).
				setSendingTime(sendingTime).
                build();
		StandardTrailer standardTrailer = StandardTrailer.newBuilder().setCheckSum("123").build();
		BidCompRspGrp bidCompRspGrp = BidCompRspGrp.newBuilder().setBidCompRspGrp(Arrays.asList(bidCompRspGrpItems)).build(); 
		return BidResponse.newBuilder().setStandardHeader(standardHeader).
				setClientBidID("bidId").
				setBidCompRspGrp(bidCompRspGrp).
				setStandardTrailer(standardTrailer).
				build();
	}
	
	/**
	 * Returns a new BidCompRspGrpItem
	 * @param commission
	 * @param currency
	 * @return
	 */
	public static BidCompRspGrpItem createBidCompRspGrpItem(String commission, String currency) {
		CommissionData commissionData = CommissionData.newBuilder().setCommCurrency(currency).build();
		return BidCompRspGrpItem.newBuilder().setCommissionData(commissionData).build();
	}
	
	/**
	 * Write the received <code>BidResponses</code> to the specified file
	 * @param bidResponses
	 * @param file
	 * @throws IOException
	 */
	public static void writeBidResponses(List<BidResponse> bidResponses, File file) throws IOException {
		DatumWriter<BidResponse> datumWriter = new SpecificDatumWriter<BidResponse>(BidResponse.class);
		if (!bidResponses.isEmpty()) {
			try(DataFileWriter<BidResponse> dataFileWriter = new DataFileWriter<BidResponse>(datumWriter)) {
				dataFileWriter.create(bidResponses.get(0).getSchema(), file);
				bidResponses.forEach(bidResponse -> {
					try {
						dataFileWriter.append(bidResponse);
					} catch (IOException e) {
						e.printStackTrace();
						throw new RuntimeException(e);
					}
				});
			}
		}
	}
	
	/**
	 * Returns a <code>List</code> of <code>BidResponse</code>s
	 * @param file
	 * @return 
	 * @throws IOException
	 */
	public static List<BidResponse> readBidResponses(File file) throws IOException {
		List<BidResponse> responses = new ArrayList<>();
		DatumReader<BidResponse> datumReader = new SpecificDatumReader<BidResponse>(BidResponse.class);
		try(DataFileReader<BidResponse> dataFileReader = new DataFileReader<BidResponse>(file, datumReader)) {
			dataFileReader.forEach(r -> {responses.add(r);System.out.println(r);});
		}
		return responses;
	}
}
