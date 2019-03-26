package com.fact;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

import com.util.KSessionSingle;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDTF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.kie.api.runtime.KieSession;
import org.kie.api.runtime.rule.FactHandle;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AccountUDTF extends GenericUDTF {
    private static Logger logger = LoggerFactory.getLogger(AccountUDTF.class);
    private Map<String, AccountFact> accountMap = new HashMap<String, AccountFact>();
    private Map<String, FactHandle> handleMap = new HashMap<String, FactHandle>();

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
        String amount = String.valueOf(data[1]);

        AccountFact accountFact = accountMap.get(account);
        KieSession kSession = KSessionSingle.getInstance();
        if (accountFact != null) {
            accountFact.setAmount(accountFact.getAmount() + Double.valueOf(amount));

            logger.info(handleMap.get(account).toExternalForm());
            kSession.update(handleMap.get(account), accountFact);
            kSession.fireAllRules();
        } else {
            logger.info("first:" + account + ":" + amount);
            accountFact = new AccountFact();
            accountFact.setAccount(account);
            accountFact.setAmount(Double.valueOf(amount));

            FactHandle handle = kSession.insert(accountFact);
            handleMap.put(account, handle);
            kSession.fireAllRules();
        }
        accountMap.put(account, accountFact);

    }

    public static void main(String[] args) throws Exception {
        AccountUDTF t = new AccountUDTF();
        t.process(new Object[]{"a", "6"});
        t.process(new Object[]{"a", "2"});
        t.process(new Object[]{"a", "-20"});
        Thread.sleep(3000);
        t.process(new Object[]{"a", "120"});
        t.process(new Object[]{"b", "5"});
        t.process(new Object[]{"b", "7"});
    }

}
