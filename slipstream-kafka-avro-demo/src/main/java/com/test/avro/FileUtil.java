package com.test.avro;

import java.io.File;
import java.util.concurrent.LinkedBlockingQueue;

public class FileUtil {
    /**
     * 创建列表，将多个文件夹下文件以及子文件夹文件加入列表中
     *
     * @param folders 文件夹集合
     * @return 文件列表
     */
    public static LinkedBlockingQueue<String> addFiles(String[] folders) {
        LinkedBlockingQueue<String> fileList = new LinkedBlockingQueue<>();
        for (String folder : folders) {
            File file = new File(folder);
            File[] files = file.listFiles();
            if (files != null) {
                for (File f : files) {
                    if (!f.isDirectory()) {
                        fileList.add(f.toString());
                    } else {
                        recursion(f.toString(), fileList);
                    }
                }
            }
        }
        return fileList;
    }

    /**
     * 辅助函数，读取子文件夹中文件
     *
     * @param root     文件夹名
     * @param fileList 文件列表
     */
    private static void recursion(String root,
                                  LinkedBlockingQueue<String> fileList) {
        File file = new File(root);
        File[] subFile = file.listFiles();
        if (subFile != null) {
            for (int i = 0; i < subFile.length; i++) {
                if (subFile[i].isDirectory()) {
                    recursion(subFile[i].getAbsolutePath(), fileList);
                } else {
                    fileList.add(subFile[i].getAbsolutePath());
                }
            }
        }
    }
}
