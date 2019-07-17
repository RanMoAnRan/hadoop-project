package zkdemo;

import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.cache.ChildData;
import org.apache.curator.framework.recipes.cache.TreeCache;
import org.apache.curator.framework.recipes.cache.TreeCacheEvent;
import org.apache.curator.framework.recipes.cache.TreeCacheListener;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.junit.Test;

/**
 * @author RanMoAnRan
 * @ClassName: ZkWatch
 * @projectName zookeeper
 * @description: TODO
 * @date 2019/6/30 22:40
 */
public class ZkWatch {

    @Test
    //Zookeeper的watch机制
    public void getWatchDatas() throws Exception {
        //1. 创建连接字符串, 这里写一个也行, 因为会同步到其他机器上.
        String connectString = "hadoop01:2181";
        //2. 设置重试机制, 3秒一次, 最多3次.
        RetryPolicy rp = new ExponentialBackoffRetry(5000, 3);

        //3. 获取CuratorFrameWork对象, 就是操作我们zk的客户端连接对象.
        CuratorFramework client = CuratorFrameworkFactory.newClient(connectString, rp);

        //4. 调用start()方法, 表示创建连接, 也就是真正意义的连上了.
        client.start();

        //获取一个具有监听功能的对象, 表示通过哪个客户端监听哪个路径(节点)
        TreeCache tc = new TreeCache(client, "/node01");

        //6. 添加监听事件.
        tc.getListenable().addListener(new TreeCacheListener() {
            //client： 操作zk的客户端
            //tce: 事件通知类型, 这个类型里边封装了节点的修改, 删除, 新增等一系列的动作.
            @Override
            public void childEvent(CuratorFramework client, TreeCacheEvent tce)throws Exception {
                //获取节点的数据
                ChildData data = tce.getData();
                if(data != null) {
                    //表明我们这个节点的数据有变化了.
                    //获取节点变化的类型, 看是新增, 修改, 还是删除
                    TreeCacheEvent.Type type = tce.getType();
                    //通过switch-case进行校验
                    switch(type) {
                        case NODE_ADDED:
                            //节点新增事件监听到了.
                            System.out.println("我监听到了节点新增事件");
                            break;
                        case NODE_UPDATED:
                            System.out.println("我监听到了节点修改事件");
                            break;
                        case INITIALIZED:
                            System.out.println("我监听到了节点的初始化事件");
                            break;
                        case NODE_REMOVED:
                            System.out.println("我监听到了节点移除事件");
                            break;
                        default:
                            System.out.println("什么也不做");
                            break;
                    }
                }
            }
        });

        //7. 开始监听.
        tc.start();
        Thread.sleep(6000000);		//多休眠一段时间.
    }
}
