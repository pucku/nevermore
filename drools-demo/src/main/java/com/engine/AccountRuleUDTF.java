package com.engine;

import com.fact.AccountFact;
import org.apache.commons.beanutils.BeanUtils;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDTF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.drools.runtime.StatefulKnowledgeSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

public class AccountRuleUDTF extends GenericUDTF {

    private static Logger logger = LoggerFactory.getLogger(AccountRuleUDTF.class);
    private Map<String, AccountFact> accountMap = new HashMap<String, AccountFact>();
    private RuleEngineExecutor ruleEngineExecutor = new RuleEngineExecutor(true);

    public StructObjectInspector initialize(ObjectInspector[] objectInspectors) throws UDFArgumentException {
        ArrayList<String> fieldNames = new ArrayList<String>();
        ArrayList<ObjectInspector> fieldOIs = new ArrayList<ObjectInspector>();
        fieldNames.add("key");
        fieldOIs.add(PrimitiveObjectInspectorFactory.javaStringObjectInspector);

        return ObjectInspectorFactory.getStandardStructObjectInspector(fieldNames, fieldOIs);
    }

    public void process(Object[] data) throws HiveException {
        String account = String.valueOf(data[0]);
        String amount = String.valueOf(data[1]);

        AccountFact accountFact = accountMap.get(account);
        StatefulKnowledgeSession kSession = ruleEngineExecutor.getKsession();

        if (accountFact != null) {
            accountFact.setAmount(accountFact.getAmount() + Double.valueOf(amount));
            /**
             * 由于kSession在变化，所以kSession不能update FactHandle了，
             * 而同一个对象不用update FactHandle的话，是不会再次匹配规则的，
             * 此处向drools传入一个临时Fact,使得可以再次匹配规则
             */
            AccountFact tem = new AccountFact();
            try {
                BeanUtils.copyProperties(tem, accountFact);
            } catch (IllegalAccessException e) {
                e.printStackTrace();
            } catch (InvocationTargetException e) {
                e.printStackTrace();
            }
            logger.warn("update:" + accountFact);
            kSession.insert(tem);
            kSession.fireAllRules();
        } else {
            logger.info("first:" + account + ":" + amount);
            accountFact = new AccountFact();
            accountFact.setAccount(account);
            accountFact.setAmount(Double.valueOf(amount));

            kSession.insert(accountFact);
            kSession.fireAllRules();
        }
        accountMap.put(account, accountFact);
    }

    public void close() throws HiveException {

    }

    public static void main(String[] args) throws Exception {
        AccountRuleUDTF t = new AccountRuleUDTF();
        t.process(new Object[]{"a", "10"});
        t.process(new Object[]{"a", "-20"});
        t.process(new Object[]{"a", "-30"});

        t.process(new Object[]{"a", "120"});
        Thread.sleep(30 * 1000);
        t.process(new Object[]{"b", "5"});
        t.process(new Object[]{"b", "7"});
    }

}
