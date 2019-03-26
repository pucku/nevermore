package com.engine;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.drools.KnowledgeBase;
import org.drools.KnowledgeBaseFactory;
import org.drools.builder.KnowledgeBuilder;
import org.drools.builder.KnowledgeBuilderFactory;
import org.drools.io.ResourceFactory;
import org.drools.runtime.StatefulKnowledgeSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.net.URI;
import java.util.Timer;
import java.util.TimerTask;

import static org.drools.builder.ResourceType.DRL;


public class RuleEngineExecutor implements RuleEngine {

    private static Logger logger = LoggerFactory.getLogger(RuleEngineExecutor.class);
    private StatefulKnowledgeSession ksession = null;
    private Timer timer = null;
    private boolean needRefresh = false;

    public RuleEngineExecutor(boolean refresh) {
        this.needRefresh = refresh;
        initEngine();
        if (needRefresh && (timer == null)) {
            timer = new Timer();
            timer.scheduleAtFixedRate(new FlushTask(), 1000, 10000);
        }
    }

    public synchronized void initEngine() {
        try {
            KnowledgeBase kbase = readKnowledgeBase();
            ksession = kbase.newStatefulKnowledgeSession();//创建会话

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private Reader getDfsFileReader() throws Exception {
        String uri = "hdfs://nameservice1/tmp/risk.drl";
        Configuration conf = new Configuration();
        conf.addResource("core-site.xml");
        conf.addResource("hdfs-site.xml");
        conf.set("fs.hdfs.impl", "org.apache.hadoop.hdfs.DistributedFileSystem");

        FileSystem fs = FileSystem.get(URI.create(uri), conf);
        FSDataInputStream fsr = fs.open(new Path(uri));
        BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(fsr));
        return bufferedReader;
    }

    private Reader getLocalFileReader() throws Exception {
        String uri = "/home/wan/IdeaProjects/TestDroolsMQ/src/main/resources/risk/risk.drl";
        BufferedReader bufferedReader = new BufferedReader(new FileReader(new File(uri)));
        return bufferedReader;
    }

    private KnowledgeBase readKnowledgeBase() throws Exception {
        KnowledgeBuilder kbuilder = KnowledgeBuilderFactory.newKnowledgeBuilder();//创建规则构建器

        //此处可以修改文件流的输入方式，hdfs或者local
        kbuilder.add(ResourceFactory.newReaderResource(getDfsFileReader()), DRL);//加载规则文件，并增加到构建器
        KnowledgeBase kbase = KnowledgeBaseFactory.newKnowledgeBase();//创建规则构建库
        kbase.addKnowledgePackages(kbuilder.getKnowledgePackages());//构建器加载的资源文件包放入构建库

        return kbase;
    }

    public void refreshEngine() {
        logger.warn("start to refresh engine");
        initEngine();
    }

    class FlushTask extends TimerTask {
        @Override
        public void run() {
            refreshEngine();
        }
    }

    public synchronized StatefulKnowledgeSession getKsession() {
        return ksession;
    }


    public static void main(String[] args) throws Exception {
        RuleEngineExecutor t = new RuleEngineExecutor(true);
        t.initEngine();
        Thread.sleep(3 * 1000);
    }
}
