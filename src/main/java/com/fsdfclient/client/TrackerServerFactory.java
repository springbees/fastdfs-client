package com.fsdfclient.client;

import org.apache.commons.pool2.BasePooledObjectFactory;
import org.apache.commons.pool2.PooledObject;
import org.apache.commons.pool2.impl.DefaultPooledObject;
import org.csource.fastdfs.TrackerClient;
import org.csource.fastdfs.TrackerServer;

/**
 * @ClassName TrackerServerFactory
 * @Description //TODO
 * @Date 17:12 2020/5/19
 * @Author lql
 * @version 1.0
 **/
public class TrackerServerFactory extends BasePooledObjectFactory<TrackerServer>
{

    @Override
    public TrackerServer create() throws Exception
    {
        // TrackerClient
        TrackerClient trackerClient = new TrackerClient();
        // TrackerServer
        TrackerServer trackerServer = trackerClient.getConnection();

        return trackerServer;
    }

    @Override
    public PooledObject<TrackerServer> wrap(TrackerServer trackerServer)
    {
        return new DefaultPooledObject<TrackerServer>(trackerServer);
    }
}
