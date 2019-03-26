package com.test.avro;

import kafka.serializer.Encoder;
import kafka.utils.VerifiableProperties;

public class KafkaEncoder implements Encoder<byte[]> {
	public KafkaEncoder(){
		
	}
	public KafkaEncoder(VerifiableProperties pro){
		
	}
	@Override
	public byte[] toBytes(byte[] msgBytes) {
		
		
		String msgString = new String(msgBytes);
		StringBuilder stringBuilder = new StringBuilder();
		String sep = ",";
		
		try {
			String[] msgArray = msgString.split(sep);
			stringBuilder.append(msgArray[1]);
			stringBuilder.append(sep);
			stringBuilder.append(msgArray[2]);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
//		byte[] result = null;
//		ByteArrayOutputStream byteOutputStream = new ByteArrayOutputStream();
//		try {
//			ObjectOutputStream objectOutputStream = new ObjectOutputStream(byteOutputStream);
//			objectOutputStream.writeObject(stringBuilder.toString().getBytes());
//			result = byteOutputStream.toByteArray();
//		} catch (IOException e) {
//			// TODO Auto-generated catch block
//			e.printStackTrace();
//		}
//		
//		return result;
		
		return stringBuilder.toString().getBytes();
	}

}
