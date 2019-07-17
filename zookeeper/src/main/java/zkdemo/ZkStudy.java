package zkdemo;

import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.zookeeper.CreateMode;
import org.junit.Test;

/**
 * @author RanMoAnRan
 * @ClassName: ZkStudy
 * @projectName zookeeper
 * @description: TODO
 * @date 2019/6/30 21:44
 */
public class ZkStudy {
    /**
     * 创建永久节点
     *
     * @throws Exception
     */
    @Test
    public void createNode() throws Exception {
        //创建连接字符串
        String connectString = "hadoop01:2181,hadoop02:2181,hadoop03:2181";
        //设置重试机制, 3秒一次, 最多3次.
        RetryPolicy rp = new ExponentialBackoffRetry(3000, 3);

        //获取CuratorFrameWork对象, 就是操作我们zk的客户端连接对象.
        CuratorFramework client = CuratorFrameworkFactory.newClient(connectString, rp);
        //调用start()方法, 表示创建连接, 也就是真正意义的连上了.
        client.start();
        //创建永久节点.
        client.create().creatingParentsIfNeeded().withMode(CreateMode.PERSISTENT).forPath("/node01", "helloworld".getBytes());
        //关闭连接.
        client.close();
    }


    /**
     * 创建临时
     *
     * @throws Exception
     */
    @Test
    public void createTempNode() throws Exception {
        //创建连接字符串
        String connectString = "hadoop01:2181,hadoop02:2181,hadoop03:2181";
        //设置重试机制, 3秒一次, 最多3次.
        RetryPolicy rp = new ExponentialBackoffRetry(5000, 5);

        //获取CuratorFrameWork对象, 就是操作我们zk的客户端连接对象.
        CuratorFramework client = CuratorFrameworkFactory.newClient(connectString, rp);
        //调用start()方法, 表示创建连接, 也就是真正意义的连上了.
        client.start();
        //创建永久节点.
        client.create().creatingParentsIfNeeded().withMode(CreateMode.EPHEMERAL).forPath("/tempNode01", "helloworld".getBytes());

        Thread.sleep(5000);

        //关闭连接.
        client.close();
    }

    //修改节点数据.
    @Test
    public void delNodeData() throws Exception {
        //创建连接字符串
        String connectString = "hadoop01:2181,hadoop02:2181,hadoop03:2181";
        //设置重试机制, 3秒一次, 最多3次.
        RetryPolicy rp = new ExponentialBackoffRetry(5000, 5);

        //获取CuratorFrameWork对象, 就是操作我们zk的客户端连接对象.
        CuratorFramework client = CuratorFrameworkFactory.newClient(connectString, rp);
        //调用start()方法, 表示创建连接, 也就是真正意义的连上了.
        client.start();

        //删除节点数据.设置node01节点的值为: hahhah
        client.delete().forPath("/jing/yatou");

        Thread.sleep(5000);

        //关闭连接.
        client.close();
    }

    //修改节点数据.
    @Test
    public void updateNodeData() throws Exception {
        //创建连接字符串
        String connectString = "hadoop01:2181,hadoop02:2181,hadoop03:2181";
        //设置重试机制, 3秒一次, 最多3次.
        RetryPolicy rp = new ExponentialBackoffRetry(5000, 5);

        //获取CuratorFrameWork对象, 就是操作我们zk的客户端连接对象.
        CuratorFramework client = CuratorFrameworkFactory.newClient(connectString, rp);
        //调用start()方法, 表示创建连接, 也就是真正意义的连上了.
        client.start();

        //修改节点数据.设置node01节点的值为: jingge
        client.setData().forPath("/node01", "jingge".getBytes());

        Thread.sleep(5000);

        //关闭连接.
        client.close();
    }


    //查询节点数据
    @Test
    public void getDatas() throws Exception {
        //创建连接字符串
        String connectString = "hadoop01:2181,hadoop02:2181,hadoop03:2181";
        //设置重试机制, 3秒一次, 最多3次.
        RetryPolicy rp = new ExponentialBackoffRetry(5000, 5);

        //获取CuratorFrameWork对象, 就是操作我们zk的客户端连接对象.
        CuratorFramework client = CuratorFrameworkFactory.newClient(connectString, rp);
        //调用start()方法, 表示创建连接, 也就是真正意义的连上了.
        client.start();

        //5. 获取node01节点的值.
        byte[] bys = client.getData().forPath("/node01");
        //将其转成字符串.
        System.out.println(new String(bys));

        //关闭连接.
        client.close();
    }

}
