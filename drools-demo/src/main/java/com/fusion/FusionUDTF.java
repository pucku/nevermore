package com.fusion;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.sql.Timestamp;
import java.util.ArrayList;

import com.util.KSessionSingle;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDTF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.kie.api.runtime.KieSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FusionUDTF extends GenericUDTF {

	private static Logger logger = LoggerFactory.getLogger(FusionUDTF.class);

	@Override
	public void close() throws HiveException {
	}

	@Override
	public StructObjectInspector initialize(ObjectInspector[] arg0) throws UDFArgumentException {
		ArrayList<String> fieldNames = new ArrayList<String>();
		ArrayList<ObjectInspector> fieldOIs = new ArrayList<ObjectInspector>();
		fieldNames.add("key");
		fieldOIs.add(PrimitiveObjectInspectorFactory.javaStringObjectInspector);

		return ObjectInspectorFactory.getStandardStructObjectInspector(fieldNames, fieldOIs);
	}

	@Override
	public void process(Object[] data) throws HiveException {

		String account = String.valueOf(data[0]);
		String place = String.valueOf(data[1]);
		String op = String.valueOf(data[2]);
		String eventTime = String.valueOf(data[3]);

		TransEvent at = new TransEvent();
		at.setCard(account);
		at.setAddress(place);
		at.setBehavior(op);
		at.setEventtime(Timestamp.valueOf(eventTime));

		KieSession kSession = KSessionSingle.getInstance();
		kSession.insert(at);
		kSession.fireAllRules();
	}

	public static void main(String[] args) throws Exception {
		FusionUDTF t = new FusionUDTF();
		String fileName = "src/main/resources/data/bankbill.txt";
		FileInputStream fileInputStream = new FileInputStream(fileName);
		BufferedReader br = new BufferedReader(new InputStreamReader(fileInputStream, "UTF-8"));

		String strLine;
		while ((strLine = br.readLine()) != null) {
			String[] data = strLine.split(",");
			String address = data[0];
			String card = data[1];
			String behavior = data[2];
			String eventtime = data[3];
			t.process(new Object[] { card, address, behavior, eventtime });
		}

		br.close();
	}
}
