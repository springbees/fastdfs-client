package com.fsdfclient.myclient;

import org.csource.fastdfs.*;

import java.io.IOException;
import java.io.OutputStream;
import java.net.Socket;
import java.util.Arrays;

/**
 * @ClassName MyTrackerClient
 * @Description //TODO
 * @Date 17:11 2020/5/19
 * @Author lql
 * @version 1.0
 **/
public class MyTrackerClient
{
    protected TrackerGroup tracker_group;
    protected byte errno;

    public MyTrackerClient()
    {
        this.tracker_group = ClientGlobal.g_tracker_group;
    }

    public MyTrackerClient(TrackerGroup tracker_group)
    {
        this.tracker_group = tracker_group;
    }

    public byte getErrorCode()
    {
        return this.errno;
    }

    public TrackerServer getConnection() throws IOException
    {
        return this.tracker_group.getConnection();
    }

    public StorageServer getStoreStorage(TrackerServer trackerServer) throws IOException
    {
        String groupName = null;
        return this.getStoreStorage(trackerServer, (String) groupName, null, null);
    }

    public StorageServer getStoreStorage(TrackerServer trackerServer, String groupName, String ip, String port) throws IOException
    {
        boolean bNewConnection;
        if (trackerServer == null)
        {
            trackerServer = this.getConnection();
            if (trackerServer == null)
            {
                return null;
            }

            bNewConnection = true;
        }
        else
        {
            bNewConnection = false;
        }

        Socket trackerSocket = trackerServer.getSocket();
        OutputStream out = trackerSocket.getOutputStream();

        StorageServer var28;
        try
        {
            byte cmd;
            byte out_len;
            if (groupName != null && groupName.length() != 0)
            {
                cmd = 104;
                out_len = 16;
            }
            else
            {
                cmd = 101;
                out_len = 0;
            }

            byte[] header = ProtoCommon.packHeader(cmd, (long) out_len, (byte) 0);
            out.write(header);
            if (groupName != null && groupName.length() > 0)
            {
                byte[] bs = groupName.getBytes(ClientGlobal.g_charset);
                byte[] bGroupName = new byte[16];
                int group_len;
                if (bs.length <= 16)
                {
                    group_len = bs.length;
                }
                else
                {
                    group_len = 16;
                }

                Arrays.fill(bGroupName, (byte) 0);
                System.arraycopy(bs, 0, bGroupName, 0, group_len);
                out.write(bGroupName);
            }

            ProtoCommon.RecvPackageInfo pkgInfo = ProtoCommon.recvPackage(trackerSocket.getInputStream(), (byte) 100, 40L);
            this.errno = pkgInfo.errno;
            if (pkgInfo.errno == 0)
            {
//        String ip_addr = (new String(pkgInfo.body, 16, 15)).trim();
//        int port = (int)ProtoCommon.buff2long(pkgInfo.body, 31);
                int _port = Integer.valueOf(port);
                byte store_path = pkgInfo.body[39];
                var28 = new StorageServer(ip, _port, store_path);
                return var28;
            }

            var28 = null;
        }
        catch (IOException var25)
        {
            if (!bNewConnection)
            {
                try
                {
                    trackerServer.close();
                }
                catch (IOException var24)
                {
                    var24.printStackTrace();
                }
            }

            throw var25;
        }
        finally
        {
            if (bNewConnection)
            {
                try
                {
                    trackerServer.close();
                }
                catch (IOException var23)
                {
                    var23.printStackTrace();
                }
            }

        }

        return var28;
    }

    public StorageServer[] getStoreStorages(TrackerServer trackerServer, String groupName) throws IOException
    {
        boolean bNewConnection;
        if (trackerServer == null)
        {
            trackerServer = this.getConnection();
            if (trackerServer == null)
            {
                return null;
            }

            bNewConnection = true;
        }
        else
        {
            bNewConnection = false;
        }

        Socket trackerSocket = trackerServer.getSocket();
        OutputStream out = trackerSocket.getOutputStream();

        StorageServer[] var40;
        try
        {
            byte cmd;
            byte out_len;
            if (groupName != null && groupName.length() != 0)
            {
                cmd = 107;
                out_len = 16;
            }
            else
            {
                cmd = 106;
                out_len = 0;
            }

            byte[] header = ProtoCommon.packHeader(cmd, (long) out_len, (byte) 0);
            out.write(header);
            if (groupName != null && groupName.length() > 0)
            {
                byte[] bs = groupName.getBytes(ClientGlobal.g_charset);
                byte[] bGroupName = new byte[16];
                int group_len;
                if (bs.length <= 16)
                {
                    group_len = bs.length;
                }
                else
                {
                    group_len = 16;
                }

                Arrays.fill(bGroupName, (byte) 0);
                System.arraycopy(bs, 0, bGroupName, 0, group_len);
                out.write(bGroupName);
            }

            ProtoCommon.RecvPackageInfo pkgInfo = ProtoCommon.recvPackage(trackerSocket.getInputStream(), (byte) 100, -1L);
            this.errno = pkgInfo.errno;
            Object var37;
            if (pkgInfo.errno != 0)
            {
                var37 = null;
                return (StorageServer[]) var37;
            }

            if (pkgInfo.body.length < 40)
            {
                this.errno = 22;
                var37 = null;
                return (StorageServer[]) var37;
            }

            int ipPortLen = pkgInfo.body.length - 17;
            int recordLength = 1;
            if (ipPortLen % 23 != 0)
            {
                this.errno = 22;
                Object var39 = null;
                return (StorageServer[]) var39;
            }

            int serverCount = ipPortLen / 23;
            StorageServer[] results;
            if (serverCount > 16)
            {
                this.errno = 28;
                results = null;
                return results;
            }

            results = new StorageServer[serverCount];
            byte store_path = pkgInfo.body[pkgInfo.body.length - 1];
            int offset = 16;

            for (int i = 0; i < serverCount; ++i)
            {
                String ip_addr = (new String(pkgInfo.body, offset, 15)).trim();
                offset += 15;
                int port = (int) ProtoCommon.buff2long(pkgInfo.body, offset);
                offset += 8;
                results[i] = new StorageServer(ip_addr, port, store_path);
            }

            var40 = results;
        }
        catch (IOException var33)
        {
            if (!bNewConnection)
            {
                try
                {
                    trackerServer.close();
                }
                catch (IOException var32)
                {
                    var32.printStackTrace();
                }
            }

            throw var33;
        }
        finally
        {
            if (bNewConnection)
            {
                try
                {
                    trackerServer.close();
                }
                catch (IOException var31)
                {
                    var31.printStackTrace();
                }
            }

        }

        return var40;
    }

    public StorageServer getFetchStorage(TrackerServer trackerServer, String groupName, String filename, String ip, String port) throws IOException
    {
        ServerInfo[] servers = this.getStorages(trackerServer, (byte) 102, groupName, filename);
//    return servers == null ? null : new StorageServer(servers[0].getIpAddr(), servers[0].getPort(), 0);
        int _port = Integer.valueOf(port);
        return servers == null ? null : new StorageServer(ip, _port, 0);
    }

    public StorageServer getUpdateStorage(TrackerServer trackerServer, String groupName, String filename, String ip, String port) throws IOException
    {
        ServerInfo[] servers = this.getStorages(trackerServer, (byte) 103, groupName, filename);
        int _port = Integer.valueOf(port);
        return servers == null ? null : new StorageServer(ip, _port, 0);
    }

    public ServerInfo[] getFetchStorages(TrackerServer trackerServer, String groupName, String filename) throws IOException
    {
        return this.getStorages(trackerServer, (byte) 105, groupName, filename);
    }

    protected ServerInfo[] getStorages(TrackerServer trackerServer, byte cmd, String groupName, String filename) throws IOException
    {
        boolean bNewConnection;
        if (trackerServer == null)
        {
            trackerServer = this.getConnection();
            if (trackerServer == null)
            {
                return null;
            }

            bNewConnection = true;
        }
        else
        {
            bNewConnection = false;
        }

        Socket trackerSocket = trackerServer.getSocket();
        OutputStream out = trackerSocket.getOutputStream();

        Object var17;
        try
        {
            byte[] bs = groupName.getBytes(ClientGlobal.g_charset);
            byte[] bGroupName = new byte[16];
            byte[] bFileName = filename.getBytes(ClientGlobal.g_charset);
            int len;
            if (bs.length <= 16)
            {
                len = bs.length;
            }
            else
            {
                len = 16;
            }

            Arrays.fill(bGroupName, (byte) 0);
            System.arraycopy(bs, 0, bGroupName, 0, len);
            byte[] header = ProtoCommon.packHeader(cmd, (long) (16 + bFileName.length), (byte) 0);
            byte[] wholePkg = new byte[header.length + bGroupName.length + bFileName.length];
            System.arraycopy(header, 0, wholePkg, 0, header.length);
            System.arraycopy(bGroupName, 0, wholePkg, header.length, bGroupName.length);
            System.arraycopy(bFileName, 0, wholePkg, header.length + bGroupName.length, bFileName.length);
            out.write(wholePkg);
            ProtoCommon.RecvPackageInfo pkgInfo = ProtoCommon.recvPackage(trackerSocket.getInputStream(), (byte) 100, -1L);
            this.errno = pkgInfo.errno;
            if (pkgInfo.errno == 0)
            {
                if (pkgInfo.body.length < 39)
                {
                    throw new IOException("Invalid body length: " + pkgInfo.body.length);
                }

                if ((pkgInfo.body.length - 39) % 15 != 0)
                {
                    throw new IOException("Invalid body length: " + pkgInfo.body.length);
                }

                int server_count = 1 + (pkgInfo.body.length - 39) / 15;
                String ip_addr = (new String(pkgInfo.body, 16, 15)).trim();
                int offset = 31;
                int port = (int) ProtoCommon.buff2long(pkgInfo.body, offset);
                offset = offset + 8;
                ServerInfo[] servers = new ServerInfo[server_count];
                servers[0] = new ServerInfo(ip_addr, port);

                for (int i = 1; i < server_count; ++i)
                {
                    servers[i] = new ServerInfo((new String(pkgInfo.body, offset, 15)).trim(), port);
                    offset += 15;
                }

                ServerInfo[] var36 = servers;
                return var36;
            }

            var17 = null;
        }
        catch (IOException var32)
        {
            if (!bNewConnection)
            {
                try
                {
                    trackerServer.close();
                }
                catch (IOException var31)
                {
                    var31.printStackTrace();
                }
            }

            throw var32;
        }
        finally
        {
            if (bNewConnection)
            {
                try
                {
                    trackerServer.close();
                }
                catch (IOException var30)
                {
                    var30.printStackTrace();
                }
            }

        }

        return (ServerInfo[]) var17;
    }

    public StorageServer getFetchStorage1(TrackerServer trackerServer, String file_id, String ip, String port) throws IOException
    {
        String[] parts = new String[2];
        this.errno = StorageClient2.split_file_id(file_id, parts);
        return this.errno != 0 ? null : this.getFetchStorage(trackerServer, parts[0], parts[1], ip, port);
    }

    public ServerInfo[] getFetchStorages1(TrackerServer trackerServer, String file_id) throws IOException
    {
        String[] parts = new String[2];
        this.errno = StorageClient2.split_file_id(file_id, parts);
        return this.errno != 0 ? null : this.getFetchStorages(trackerServer, parts[0], parts[1]);
    }

    public StructGroupStat[] listGroups(TrackerServer trackerServer) throws IOException
    {
        boolean bNewConnection;
        if (trackerServer == null)
        {
            trackerServer = this.getConnection();
            if (trackerServer == null)
            {
                return null;
            }

            bNewConnection = true;
        }
        else
        {
            bNewConnection = false;
        }

        Socket trackerSocket = trackerServer.getSocket();
        OutputStream out = trackerSocket.getOutputStream();

        ProtoStructDecoder decoder;
        try
        {
            byte[] header = ProtoCommon.packHeader((byte) 91, 0L, (byte) 0);
            out.write(header);
            ProtoCommon.RecvPackageInfo pkgInfo = ProtoCommon.recvPackage(trackerSocket.getInputStream(), (byte) 100, -1L);
            this.errno = pkgInfo.errno;
            if (pkgInfo.errno != 0)
            {
//        decoder = null;
//        return decoder;
                return null;
            }

            decoder = new ProtoStructDecoder();
            StructGroupStat[] var13 = (StructGroupStat[]) decoder.decode(pkgInfo.body, StructGroupStat.class, StructGroupStat.getFieldsTotalSize());
            return var13;
        }
        catch (IOException var27)
        {
            if (!bNewConnection)
            {
                try
                {
                    trackerServer.close();
                }
                catch (IOException var26)
                {
                    var26.printStackTrace();
                }
            }

            throw var27;
        }
        catch (Exception var28)
        {
            var28.printStackTrace();
            this.errno = 22;
            decoder = null;
        }
        finally
        {
            if (bNewConnection)
            {
                try
                {
                    trackerServer.close();
                }
                catch (IOException var25)
                {
                    var25.printStackTrace();
                }
            }

        }
        return null;
//    return decoder;
    }

    public StructStorageStat[] listStorages(TrackerServer trackerServer, String groupName) throws IOException
    {
        String storageIpAddr = null;
        return this.listStorages(trackerServer, groupName, (String) storageIpAddr);
    }

    public StructStorageStat[] listStorages(TrackerServer trackerServer, String groupName, String storageIpAddr) throws IOException
    {
        boolean bNewConnection;
        if (trackerServer == null)
        {
            trackerServer = this.getConnection();
            if (trackerServer == null)
            {
                return null;
            }

            bNewConnection = true;
        }
        else
        {
            bNewConnection = false;
        }

        Socket trackerSocket = trackerServer.getSocket();
        OutputStream out = trackerSocket.getOutputStream();

        ProtoStructDecoder decoder;
        try
        {
            byte[] bs = groupName.getBytes(ClientGlobal.g_charset);
            byte[] bGroupName = new byte[16];
            int len;
            if (bs.length <= 16)
            {
                len = bs.length;
            }
            else
            {
                len = 16;
            }

            Arrays.fill(bGroupName, (byte) 0);
            System.arraycopy(bs, 0, bGroupName, 0, len);
            byte[] bIpAddr;
            int ipAddrLen;
            if (storageIpAddr != null && storageIpAddr.length() > 0)
            {
                bIpAddr = storageIpAddr.getBytes(ClientGlobal.g_charset);
                if (bIpAddr.length < 16)
                {
                    ipAddrLen = bIpAddr.length;
                }
                else
                {
                    ipAddrLen = 15;
                }
            }
            else
            {
                bIpAddr = null;
                ipAddrLen = 0;
            }

            byte[] header = ProtoCommon.packHeader((byte) 92, (long) (16 + ipAddrLen), (byte) 0);
            byte[] wholePkg = new byte[header.length + bGroupName.length + ipAddrLen];
            System.arraycopy(header, 0, wholePkg, 0, header.length);
            System.arraycopy(bGroupName, 0, wholePkg, header.length, bGroupName.length);
            if (ipAddrLen > 0)
            {
                System.arraycopy(bIpAddr, 0, wholePkg, header.length + bGroupName.length, ipAddrLen);
            }

            out.write(wholePkg);
            ProtoCommon.RecvPackageInfo pkgInfo = ProtoCommon.recvPackage(trackerSocket.getInputStream(), (byte) 100, -1L);
            this.errno = pkgInfo.errno;
            if (pkgInfo.errno == 0)
            {
                decoder = new ProtoStructDecoder();
                StructStorageStat[] var16 = (StructStorageStat[]) decoder.decode(pkgInfo.body, StructStorageStat.class, StructStorageStat.getFieldsTotalSize());
                return var16;
            }

            decoder = null;
        }
        catch (IOException var30)
        {
            if (!bNewConnection)
            {
                try
                {
                    trackerServer.close();
                }
                catch (IOException var29)
                {
                    var29.printStackTrace();
                }
            }

            throw var30;
        }
        catch (Exception var31)
        {
            var31.printStackTrace();
            this.errno = 22;
            Object var12 = null;
            return (StructStorageStat[]) var12;
        }
        finally
        {
            if (bNewConnection)
            {
                try
                {
                    trackerServer.close();
                }
                catch (IOException var28)
                {
                    var28.printStackTrace();
                }
            }

        }
        return null;
//    return decoder;
    }

    private boolean deleteStorage(TrackerServer trackerServer, String groupName, String storageIpAddr) throws IOException
    {
        Socket trackerSocket = trackerServer.getSocket();
        OutputStream out = trackerSocket.getOutputStream();
        byte[] bs = groupName.getBytes(ClientGlobal.g_charset);
        byte[] bGroupName = new byte[16];
        int len;
        if (bs.length <= 16)
        {
            len = bs.length;
        }
        else
        {
            len = 16;
        }

        Arrays.fill(bGroupName, (byte) 0);
        System.arraycopy(bs, 0, bGroupName, 0, len);
        byte[] bIpAddr = storageIpAddr.getBytes(ClientGlobal.g_charset);
        int ipAddrLen;
        if (bIpAddr.length < 16)
        {
            ipAddrLen = bIpAddr.length;
        }
        else
        {
            ipAddrLen = 15;
        }

        byte[] header = ProtoCommon.packHeader((byte) 93, (long) (16 + ipAddrLen), (byte) 0);
        byte[] wholePkg = new byte[header.length + bGroupName.length + ipAddrLen];
        System.arraycopy(header, 0, wholePkg, 0, header.length);
        System.arraycopy(bGroupName, 0, wholePkg, header.length, bGroupName.length);
        System.arraycopy(bIpAddr, 0, wholePkg, header.length + bGroupName.length, ipAddrLen);
        out.write(wholePkg);
        ProtoCommon.RecvPackageInfo pkgInfo = ProtoCommon.recvPackage(trackerSocket.getInputStream(), (byte) 100, 0L);
        this.errno = pkgInfo.errno;
        return pkgInfo.errno == 0;
    }

    public boolean deleteStorage(String groupName, String storageIpAddr) throws IOException
    {
        return this.deleteStorage(ClientGlobal.g_tracker_group, groupName, storageIpAddr);
    }

    public boolean deleteStorage(TrackerGroup trackerGroup, String groupName, String storageIpAddr) throws IOException
    {
        int notFoundCount = 0;

        int serverIndex;
        TrackerServer trackerServer;
        for (serverIndex = 0; serverIndex < trackerGroup.tracker_servers.length; ++serverIndex)
        {
            try
            {
                trackerServer = trackerGroup.getConnection(serverIndex);
            }
            catch (IOException var37)
            {
                var37.printStackTrace(System.err);
                this.errno = 61;
                return false;
            }

            boolean var8;
            try
            {
                StructStorageStat[] storageStats = this.listStorages(trackerServer, groupName, storageIpAddr);
                if (storageStats == null)
                {
                    if (this.errno != 2)
                    {
                        var8 = false;
                        return var8;
                    }

                    ++notFoundCount;
                    continue;
                }

                if (storageStats.length == 0)
                {
                    ++notFoundCount;
                    continue;
                }

                if (storageStats[0].getStatus() != 6 && storageStats[0].getStatus() != 7)
                {
                    continue;
                }

                this.errno = 16;
                var8 = false;
            }
            finally
            {
                try
                {
                    trackerServer.close();
                }
                catch (IOException var34)
                {
                    var34.printStackTrace();
                }

            }

            return var8;
        }

        if (notFoundCount == trackerGroup.tracker_servers.length)
        {
            this.errno = 2;
            return false;
        }
        else
        {
            notFoundCount = 0;

            for (serverIndex = 0; serverIndex < trackerGroup.tracker_servers.length; ++serverIndex)
            {
                try
                {
                    trackerServer = trackerGroup.getConnection(serverIndex);
                }
                catch (IOException var36)
                {
                    System.err.println("connect to server " + trackerGroup.tracker_servers[serverIndex].getAddress().getHostAddress() + ":" + trackerGroup.tracker_servers[serverIndex].getPort() + " fail");
                    var36.printStackTrace(System.err);
                    this.errno = 61;
                    return false;
                }

                boolean var7;
                try
                {
                    if (this.deleteStorage(trackerServer, groupName, storageIpAddr) || this.errno == 0)
                    {
                        continue;
                    }

                    if (this.errno == 2)
                    {
                        ++notFoundCount;
                        continue;
                    }

                    if (this.errno == 114)
                    {
                        continue;
                    }

                    var7 = false;
                }
                finally
                {
                    try
                    {
                        trackerServer.close();
                    }
                    catch (IOException var35)
                    {
                        var35.printStackTrace();
                    }

                }

                return var7;
            }

            if (notFoundCount == trackerGroup.tracker_servers.length)
            {
                this.errno = 2;
                return false;
            }
            else
            {
                if (this.errno == 2)
                {
                    this.errno = 0;
                }

                return this.errno == 0;
            }
        }
    }
}
