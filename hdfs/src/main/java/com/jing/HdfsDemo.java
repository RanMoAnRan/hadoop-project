package com.jing;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.testng.annotations.Test;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.net.URI;


/**
 * @author RanMoAnRan
 * @ClassName: HdfsDemo
 * @projectName hadoop-project
 * @description: TODO
 * @date 2019/7/2 21:14
 */
public class HdfsDemo {

    /**
     * 通过FileSystem获取分布式文件系统(HDFS)
     * @throws Exception
     */
    @Test
    public void getFileSystem() throws Exception {
        FileSystem fileSystem = FileSystem.get(new URI("hdfs://hadoop01:8020/"), new Configuration());
        System.out.println(fileSystem);
    }

    @Test
    public void getFileSystem2() throws Exception {
        FileSystem fileSystem = FileSystem.newInstance(new URI("hdfs://hadoop01:8020/"), new Configuration());
        System.out.println(fileSystem);
    }


    //递归遍历hdfs当中所有的文件路径
    @Test
    public void getAllHdfsFilePath() throws Exception {
        //获取分布式文件系统的客户端.
        FileSystem fileSystem = FileSystem.get(new URI("hdfs://hadoop01:8020"), new Configuration());

        //给定我们hdfs的根路径
        Path path = new Path("/");

        //调用FileSystem#listStatus()方法, 获取根路径下所有文件(夹)的状态
        FileStatus[] fileStatuses = fileSystem.listStatus(path);
        //循环遍历我们的fileStatus, 如果是文件就打印路径, 如果是文件夹, 就递归
        for (FileStatus fileStatus : fileStatuses) {
            //是文件夹就递归
            if (fileStatus.isDirectory()) {
                getDirectoryFile(fileSystem, fileStatus);
            }else {
                //打印文件路径
                Path statusPath = fileStatus.getPath();
                System.out.println(statusPath);
            }
        }

        fileSystem.close();
    }

    //递归文件方法
    private void getDirectoryFile(FileSystem fileSystem, FileStatus fileStatus) throws IOException {
        FileStatus[] fileStatuses = fileSystem.listStatus(fileStatus.getPath());
        for (FileStatus status : fileStatuses) {
            if (status.isDirectory()) {
                getDirectoryFile(fileSystem,status);
            }else {
                System.out.println(status.getPath());
            }
        }
    }



    //递归遍历hdfs当中所有的文件路径方式二
    @Test
    public void getAllHdfsFilePath2() throws Exception {
        //获取分布式文件系统的客户端.
        FileSystem fileSystem = FileSystem.get(new URI("hdfs://hadoop01:8020"), new Configuration());

        //给定我们hdfs的根路径
        Path path = new Path("/");

        //调用FileSystem#listSFiles()方法, 获取根路径下所有文件(夹)的状态
        RemoteIterator<LocatedFileStatus> listFiles = fileSystem.listFiles(path, true);

        //循环遍历我们的fileStatus, 如果是文件就打印路径, 如果是文件夹, 就递归
        while (listFiles.hasNext()){
            LocatedFileStatus fileStatus = listFiles.next();
            System.out.println(fileStatus.getPath());
        }

        fileSystem.close();
    }


    //下载hdfs文件到本地
    @Test
    public void copyHdfsToLocal() throws Exception {
        FileSystem fileSystem = FileSystem.get(new URI("hdfs://hadoop01:8020"), new Configuration());
        Path path = new Path("/test/input/install.log");
        FSDataInputStream dataInputStream = fileSystem.open(path);
        FileOutputStream outputStream = new FileOutputStream("d:/myInstall.txt");
        IOUtils.copy(dataInputStream,outputStream);
        IOUtils.closeQuietly(dataInputStream);
        IOUtils.closeQuietly(outputStream);

        fileSystem.close();
    }

    //在hdfs上面创建文件夹
    @Test
    public void createHdfsDir() throws Exception {
        FileSystem fileSystem = FileSystem.get(new URI("hdfs://hadoop01:8020"), new Configuration());
        //创建文件夹
        fileSystem.mkdirs(new Path("/a/b/c"));
        fileSystem.close();
    }



    //在hdfs上面删除文件夹或文件
    @Test
    public void deleteHdfsDir() throws Exception {
        FileSystem fileSystem = FileSystem.get(new URI("hdfs://hadoop01:8020"), new Configuration());
        //删除文件夹或文件,有子文件也会同时删除
        fileSystem.delete(new Path("/a/b"),true);
        fileSystem.close();
    }


    //Hdfs的文件上传操作
    @Test
    public void uploadFileToHdfs() throws Exception {
        //1. 获取分布式文件系统的客户端.  如果开启了hadoop的权限验证，则需要伪装自己是root账号进行上传操作
        FileSystem fileSystem = FileSystem.get(new URI("hdfs://hadoop01:8020"), new Configuration(),"root");
        //false: 不删除原文件.
        fileSystem.copyFromLocalFile(false, new Path("file:///d:\\7c1ed21b0ef41bd5d620898557da81cb39db3d4b.jpg"), new Path("/a/b"));

        fileSystem.close();
    }


    //将多个小文件合并成一个大文件,文件类型必须相同
    @Test
    public void mergeFile()throws Exception {
        FileSystem fileSystem = FileSystem.get(new URI("hdfs://hadoop01:8020"), new Configuration(), "root");

        //1. 在hdfs上创建一个文件.
        FSDataOutputStream os = fileSystem.create(new Path("/bigfile.xml"));

        //2.获取本地所有小文件的输入流.
        LocalFileSystem local = fileSystem.getLocal(new Configuration());
        FileStatus[] fileStatuses = local.listStatus(new Path("D:\\test"));

        //3. 遍历, 获取到每一个小文件的路径
        for (FileStatus fileStatus : fileStatuses) {
            Path path = fileStatus.getPath();
            FSDataInputStream is = local.open(path);
            IOUtils.copy(is, os);
            IOUtils.closeQuietly(is);
        }

        //4. 关流
        IOUtils.closeQuietly(os);
        fileSystem.close();
        local.close();
    }

}
