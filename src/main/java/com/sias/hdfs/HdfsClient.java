package com.sias.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

/**
 * @author Edgar
 * @create 2022-07-17 21:15
 * @faction:
 */
public class HdfsClient {

    private FileSystem fileSystem;

    @Before
    public void init() throws URISyntaxException, IOException, InterruptedException {
        /*01.连接地址*/
        URI uri = new URI("hdfs://hadoop102:8020");

        /*02.创建一个配置文件
         *    就是可以去操作文件了*/
        Configuration entries = new Configuration();

        /*03.增加用户*/
        String user ="sias";
        /*04.获取客户端对象*/
        fileSystem = FileSystem.get(uri, entries,user);
    }

    @After
    public void close() throws IOException {
        fileSystem.close();
    }

    @Test
    public void mkdir() throws URISyntaxException, IOException, InterruptedException {
        /*05.创建文件并且关闭资源*/
        fileSystem.mkdirs(new Path("/Hadoop/xiyouji"));
    }


    /*1.上传文件*/
    @Test
    public void upload() throws IOException {
        /*01.第一个参数，是否删除原数据，第二个，是否覆盖，第三个要长传的文件，第四个上传的位置*/
        fileSystem.copyFromLocalFile(false,true,new Path("D:\\User1\\rundata\\document\\major\\UnderASophomore\\Test\\suwukong.txt"),new Path("/Hadoop"));
    }
}
