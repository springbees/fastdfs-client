package com.fsdfclient.myclient;

import org.csource.common.Base64;
import org.csource.common.MyException;
import org.csource.common.NameValuePair;
import org.csource.fastdfs.*;

import java.io.*;
import java.net.Socket;
import java.util.Arrays;

/**
 * @ClassName MyStorageClient
 * @Description //TODO
 * @Date 17:11 2020/5/19
 * @Author lql
 * @version 1.0
 **/
public class MyStorageClient
{

    public static final Base64 base64 = new Base64('-', '_', '.', 0);
    protected TrackerServer trackerServer;
    protected StorageServer storageServer;
    protected byte errno;

    public MyStorageClient()
    {
        this.trackerServer = null;
        this.storageServer = null;
    }

    public MyStorageClient(TrackerServer trackerServer, StorageServer storageServer)
    {
        this.trackerServer = trackerServer;
        this.storageServer = storageServer;
    }

    public byte getErrorCode()
    {
        return this.errno;
    }

    public String[] upload_file(String local_filename, String file_ext_name, NameValuePair[] meta_list, String ip, String port) throws IOException, MyException
    {
        String group_name = null;
        return this.upload_file((String) group_name, (String) local_filename, file_ext_name, meta_list, ip, port);
    }

    protected String[] upload_file(String group_name, String local_filename, String file_ext_name, NameValuePair[] meta_list, String ip, String port) throws IOException, MyException
    {
        byte cmd = 0x01;
        return this.upload_file((byte) 11, group_name, local_filename, file_ext_name, meta_list, ip, port);
    }

    protected String[] upload_file(byte cmd, String group_name, String local_filename, String file_ext_name, NameValuePair[] meta_list, String ip, String port) throws IOException, MyException
    {
        File f = new File(local_filename);
        FileInputStream fis = new FileInputStream(f);
        if (file_ext_name == null)
        {
            int nPos = local_filename.lastIndexOf(46);
            if (nPos > 0 && local_filename.length() - nPos <= 7)
            {
                file_ext_name = local_filename.substring(nPos + 1);
            }
        }

        String[] var12;
        try
        {
            var12 = this.do_upload_file(cmd, group_name, (String) null, (String) null, file_ext_name, f.length(), new UploadStream(fis, f.length()), meta_list, ip, port);
        }
        finally
        {
            fis.close();
        }

        return var12;
    }

    public String[] upload_file(byte[] file_buff, int offset, int length, String file_ext_name, NameValuePair[] meta_list, String ip, String port) throws IOException, MyException
    {
        String group_name = null;
        return this.upload_file((String) group_name, file_buff, offset, length, file_ext_name, meta_list, ip, port);
    }

    public String[] upload_file(String group_name, byte[] file_buff, int offset, int length, String file_ext_name, NameValuePair[] meta_list, String ip, String port) throws IOException, MyException
    {
        return this.do_upload_file((byte) 11, group_name, (String) null, (String) null, file_ext_name, (long) length, new StorageClient.UploadBuff(file_buff, offset, length), meta_list, ip, port);
    }

    public String[] upload_file(byte[] file_buff, String file_ext_name, NameValuePair[] meta_list, String ip, String port) throws IOException, MyException
    {
        String group_name = null;
        return this.upload_file((String) group_name, file_buff, 0, file_buff.length, file_ext_name, meta_list, ip, port);
    }

    public String[] upload_file(String group_name, byte[] file_buff, String file_ext_name, NameValuePair[] meta_list, String ip, String port) throws IOException, MyException
    {
        return this.do_upload_file((byte) 11, group_name, (String) null, (String) null, file_ext_name, (long) file_buff.length, new StorageClient.UploadBuff(file_buff, 0, file_buff.length), meta_list, ip, port);
    }

    public String[] upload_file(String group_name, long file_size, UploadCallback callback, String file_ext_name, NameValuePair[] meta_list, String ip, String port) throws IOException, MyException
    {
        String master_filename = null;
        String prefix_name = null;
        return this.do_upload_file((byte) 11, group_name, (String) master_filename, (String) prefix_name, file_ext_name, file_size, callback, meta_list, ip, port);
    }

    public String[] upload_file(String group_name, String master_filename, String prefix_name, String local_filename, String file_ext_name, NameValuePair[] meta_list, String ip, String port) throws IOException, MyException
    {
        if (group_name != null && group_name.length() != 0 && master_filename != null && master_filename.length() != 0 && prefix_name != null)
        {
            File f = new File(local_filename);
            FileInputStream fis = new FileInputStream(f);
            if (file_ext_name == null)
            {
                int nPos = local_filename.lastIndexOf(46);
                if (nPos > 0 && local_filename.length() - nPos <= 7)
                {
                    file_ext_name = local_filename.substring(nPos + 1);
                }
            }

            String[] var13;
            try
            {
                var13 = this.do_upload_file((byte) 21, group_name, master_filename, prefix_name, file_ext_name, f.length(), new UploadStream(fis, f.length()), meta_list, ip, port);
            }
            finally
            {
                fis.close();
            }

            return var13;
        }
        else
        {
            throw new MyException("invalid arguement");
        }
    }

    public String[] upload_file(String group_name, String master_filename, String prefix_name, byte[] file_buff, String file_ext_name, NameValuePair[] meta_list, String ip, String port) throws IOException, MyException
    {
        if (group_name != null && group_name.length() != 0 && master_filename != null && master_filename.length() != 0 && prefix_name != null)
        {
            return this.do_upload_file((byte) 21, group_name, master_filename, prefix_name, file_ext_name, (long) file_buff.length, new StorageClient.UploadBuff(file_buff, 0, file_buff.length), meta_list, ip, port);
        }
        else
        {
            throw new MyException("invalid arguement");
        }
    }

    public String[] upload_file(String group_name, String master_filename, String prefix_name, byte[] file_buff, int offset, int length, String file_ext_name, NameValuePair[] meta_list, String ip, String port) throws IOException, MyException
    {
        if (group_name != null && group_name.length() != 0 && master_filename != null && master_filename.length() != 0 && prefix_name != null)
        {
            return this.do_upload_file((byte) 21, group_name, master_filename, prefix_name, file_ext_name, (long) length, new StorageClient.UploadBuff(file_buff, offset, length), meta_list, ip, port);
        }
        else
        {
            throw new MyException("invalid arguement");
        }
    }

    public String[] upload_file(String group_name, String master_filename, String prefix_name, long file_size, UploadCallback callback, String file_ext_name, NameValuePair[] meta_list, String ip, String port) throws IOException, MyException
    {
        return this.do_upload_file((byte) 21, group_name, master_filename, prefix_name, file_ext_name, file_size, callback, meta_list, ip, port);
    }

    public String[] upload_appender_file(String local_filename, String file_ext_name, NameValuePair[] meta_list, String ip, String port) throws IOException, MyException
    {
        String group_name = null;
        return this.upload_appender_file((String) group_name, (String) local_filename, file_ext_name, meta_list, ip, port);
    }

    protected String[] upload_appender_file(String group_name, String local_filename, String file_ext_name, NameValuePair[] meta_list, String ip, String port) throws IOException, MyException
    {
        byte cmd = 0x01;
        return this.upload_file((byte) 23, group_name, local_filename, file_ext_name, meta_list, ip, port);
    }

    public String[] upload_appender_file(byte[] file_buff, int offset, int length, String file_ext_name, NameValuePair[] meta_list, String ip, String port) throws IOException, MyException
    {
        String group_name = null;
        return this.upload_appender_file((String) group_name, file_buff, offset, length, file_ext_name, meta_list, ip, port);
    }

    public String[] upload_appender_file(String group_name, byte[] file_buff, int offset, int length, String file_ext_name, NameValuePair[] meta_list, String ip, String port) throws IOException, MyException
    {
        return this.do_upload_file((byte) 23, group_name, (String) null, (String) null, file_ext_name, (long) length, new StorageClient.UploadBuff(file_buff, offset, length), meta_list, ip, port);
    }

    public String[] upload_appender_file(byte[] file_buff, String file_ext_name, NameValuePair[] meta_list, String ip, String port) throws IOException, MyException
    {
        String group_name = null;
        return this.upload_appender_file((String) group_name, file_buff, 0, file_buff.length, file_ext_name, meta_list, ip, port);
    }

    public String[] upload_appender_file(String group_name, byte[] file_buff, String file_ext_name, NameValuePair[] meta_list, String ip, String port) throws IOException, MyException
    {
        return this.do_upload_file((byte) 23, group_name, (String) null, (String) null, file_ext_name, (long) file_buff.length, new StorageClient.UploadBuff(file_buff, 0, file_buff.length), meta_list, ip, port);
    }

    public String[] upload_appender_file(String group_name, long file_size, UploadCallback callback, String file_ext_name, NameValuePair[] meta_list, String ip, String port) throws IOException, MyException
    {
        String master_filename = null;
        String prefix_name = null;
        return this.do_upload_file((byte) 23, group_name, (String) master_filename, (String) prefix_name, file_ext_name, file_size, callback, meta_list, ip, port);
    }

    public int append_file(String group_name, String appender_filename, String local_filename) throws IOException, MyException
    {
        File f = new File(local_filename);
        FileInputStream fis = new FileInputStream(f);

        int var6;
        try
        {
            var6 = this.do_append_file(group_name, appender_filename, f.length(), new UploadStream(fis, f.length()));
        }
        finally
        {
            fis.close();
        }

        return var6;
    }

    public int append_file(String group_name, String appender_filename, byte[] file_buff) throws IOException, MyException
    {
        return this.do_append_file(group_name, appender_filename, (long) file_buff.length, new StorageClient.UploadBuff(file_buff, 0, file_buff.length));
    }

    public int append_file(String group_name, String appender_filename, byte[] file_buff, int offset, int length) throws IOException, MyException
    {
        return this.do_append_file(group_name, appender_filename, (long) length, new StorageClient.UploadBuff(file_buff, offset, length));
    }

    public int append_file(String group_name, String appender_filename, long file_size, UploadCallback callback) throws IOException, MyException
    {
        return this.do_append_file(group_name, appender_filename, file_size, callback);
    }

    public int modify_file(String group_name, String appender_filename, long file_offset, String local_filename, String ip, String port) throws IOException, MyException
    {
        File f = new File(local_filename);
        FileInputStream fis = new FileInputStream(f);
        int var8;
        try
        {
            var8 = this.do_modify_file(group_name, appender_filename, file_offset, f.length(), new UploadStream(fis, f.length()), ip, port);
        }
        finally
        {
            fis.close();
        }

        return var8;
    }

    public int modify_file(String group_name, String appender_filename, long file_offset, byte[] file_buff, String ip, String port) throws IOException, MyException
    {
        return this.do_modify_file(group_name, appender_filename, file_offset, (long) file_buff.length, new StorageClient.UploadBuff(file_buff, 0, file_buff.length), ip, port);
    }

    public int modify_file(String group_name, String appender_filename, long file_offset, byte[] file_buff, int buffer_offset, int buffer_length, String ip, String port) throws IOException, MyException
    {
        return this.do_modify_file(group_name, appender_filename, file_offset, (long) buffer_length, new StorageClient.UploadBuff(file_buff, buffer_offset, buffer_length), ip, port);
    }

    public int modify_file(String group_name, String appender_filename, long file_offset, long modify_size, UploadCallback callback, String ip, String port) throws IOException, MyException
    {
        return this.do_modify_file(group_name, appender_filename, file_offset, modify_size, callback, ip, port);
    }

    protected String[] do_upload_file(byte cmd, String group_name, String master_filename, String prefix_name, String file_ext_name, long file_size, UploadCallback callback, NameValuePair[] meta_list, String ip, String port) throws IOException, MyException
    {
        boolean bUploadSlave = group_name != null && group_name.length() > 0 && master_filename != null && master_filename.length() > 0 && prefix_name != null;
        boolean bNewConnection;
        if (bUploadSlave)
        {
            bNewConnection = this.newUpdatableStorageConnection(group_name, master_filename, ip, port);
        }
        else
        {
            bNewConnection = this.newWritableStorageConnection(group_name, ip, port);
        }

        String[] var28;
        try
        {
            Socket storageSocket = this.storageServer.getSocket();
            byte[] ext_name_bs = new byte[6];
            Arrays.fill(ext_name_bs, (byte) 0);
            if (file_ext_name != null && file_ext_name.length() > 0)
            {
                byte[] bs = file_ext_name.getBytes(ClientGlobal.g_charset);
                int ext_name_len = bs.length;
                if (ext_name_len > 6)
                {
                    ext_name_len = 6;
                }

                System.arraycopy(bs, 0, ext_name_bs, 0, ext_name_len);
            }

            byte[] sizeBytes;
            byte[] hexLenBytes;
            byte[] masterFilenameBytes;
            int offset;
            long body_len;
            if (bUploadSlave)
            {
                masterFilenameBytes = master_filename.getBytes(ClientGlobal.g_charset);
                sizeBytes = new byte[16];
                body_len = (long) (sizeBytes.length + 16 + 6 + masterFilenameBytes.length) + file_size;
                hexLenBytes = ProtoCommon.long2buff((long) master_filename.length());
                System.arraycopy(hexLenBytes, 0, sizeBytes, 0, hexLenBytes.length);
                offset = hexLenBytes.length;
            }
            else
            {
                masterFilenameBytes = null;
                sizeBytes = new byte[9];
                body_len = (long) (sizeBytes.length + 6) + file_size;
                sizeBytes[0] = (byte) this.storageServer.getStorePathIndex();
                offset = 1;
            }

            hexLenBytes = ProtoCommon.long2buff(file_size);
            System.arraycopy(hexLenBytes, 0, sizeBytes, offset, hexLenBytes.length);
            OutputStream out = storageSocket.getOutputStream();
            byte[] header = ProtoCommon.packHeader(cmd, body_len, (byte) 0);
            byte[] wholePkg = new byte[(int) ((long) header.length + body_len - file_size)];
            System.arraycopy(header, 0, wholePkg, 0, header.length);
            System.arraycopy(sizeBytes, 0, wholePkg, header.length, sizeBytes.length);
            offset = header.length + sizeBytes.length;
            int result;
            if (bUploadSlave)
            {
                byte[] prefix_name_bs = new byte[16];
                byte[] bs = prefix_name.getBytes(ClientGlobal.g_charset);
                result = bs.length;
                Arrays.fill(prefix_name_bs, (byte) 0);
                if (result > 16)
                {
                    result = 16;
                }

                if (result > 0)
                {
                    System.arraycopy(bs, 0, prefix_name_bs, 0, result);
                }

                System.arraycopy(prefix_name_bs, 0, wholePkg, offset, prefix_name_bs.length);
                offset += prefix_name_bs.length;
            }

            System.arraycopy(ext_name_bs, 0, wholePkg, offset, ext_name_bs.length);
            offset += ext_name_bs.length;
            if (bUploadSlave)
            {
                System.arraycopy(masterFilenameBytes, 0, wholePkg, offset, masterFilenameBytes.length);
                int var10000 = offset + masterFilenameBytes.length;
            }

            out.write(wholePkg);
            ProtoCommon.RecvPackageInfo pkgInfo;
            if ((this.errno = (byte) callback.send(out)) != 0)
            {
                return null;
            }

            pkgInfo = ProtoCommon.recvPackage(storageSocket.getInputStream(), (byte) 100, -1L);
            this.errno = pkgInfo.errno;
            String[] results;
            if (pkgInfo.errno != 0)
            {
                results = null;
                return results;
            }

            if (pkgInfo.body.length <= 16)
            {
                throw new MyException("body length: " + pkgInfo.body.length + " <= " + 16);
            }

            String new_group_name = (new String(pkgInfo.body, 0, 16)).trim();
            String remote_filename = new String(pkgInfo.body, 16, pkgInfo.body.length - 16);
            results = new String[]{new_group_name, remote_filename};
            if (meta_list == null || meta_list.length == 0)
            {
                String[] var230 = results;
                return var230;
            }

            result = 0;
            boolean var217 = false;

            try
            {
                var217 = true;
                result = this.set_metadata(new_group_name, remote_filename, meta_list, (byte) 79);
                var217 = false;
            }
            catch (IOException var222)
            {
                result = 5;
                throw var222;
            }
            finally
            {
                if (var217)
                {
                    if (result != 0)
                    {
                        this.errno = (byte) result;
                        this.delete_file(new_group_name, remote_filename);
                        Object var33 = null;
                        return (String[]) var33;
                    }
                }
            }

            if (result == 0)
            {
                var28 = results;
                return var28;
            }

            this.errno = (byte) result;
            this.delete_file(new_group_name, remote_filename);
            var28 = null;
        }
        catch (IOException var224)
        {
            if (!bNewConnection)
            {
                try
                {
                    this.storageServer.close();
                }
                catch (IOException var220)
                {
                    var220.printStackTrace();
                }
                finally
                {
                    this.storageServer = null;
                }
            }

            throw var224;
        }
        finally
        {
            if (bNewConnection)
            {
                try
                {
                    this.storageServer.close();
                }
                catch (IOException var218)
                {
                    var218.printStackTrace();
                }
                finally
                {
                    this.storageServer = null;
                }
            }

        }

        return var28;
    }

    protected int do_append_file(String group_name, String appender_filename, long file_size, UploadCallback callback) throws IOException, MyException
    {
        if (group_name != null && group_name.length() != 0 && appender_filename != null && appender_filename.length() != 0)
        {
            String ip = null;
            String port = null;
            boolean bNewConnection = this.newUpdatableStorageConnection(group_name, appender_filename, ip, port);

            byte var16;
            try
            {
                Socket storageSocket = this.storageServer.getSocket();
                byte[] appenderFilenameBytes = appender_filename.getBytes(ClientGlobal.g_charset);
                long body_len = (long) (16 + appenderFilenameBytes.length) + file_size;
                byte[] header = ProtoCommon.packHeader((byte) 24, body_len, (byte) 0);
                byte[] wholePkg = new byte[(int) ((long) header.length + body_len - file_size)];
                System.arraycopy(header, 0, wholePkg, 0, header.length);
                int offset = header.length;
                byte[] hexLenBytes = ProtoCommon.long2buff((long) appender_filename.length());
                System.arraycopy(hexLenBytes, 0, wholePkg, offset, hexLenBytes.length);
                offset += hexLenBytes.length;
                hexLenBytes = ProtoCommon.long2buff(file_size);
                System.arraycopy(hexLenBytes, 0, wholePkg, offset, hexLenBytes.length);
                offset += hexLenBytes.length;
                OutputStream out = storageSocket.getOutputStream();
                System.arraycopy(appenderFilenameBytes, 0, wholePkg, offset, appenderFilenameBytes.length);
                int var10000 = offset + appenderFilenameBytes.length;
                out.write(wholePkg);
                if ((this.errno = (byte) callback.send(out)) == 0)
                {
                    ProtoCommon.RecvPackageInfo pkgInfo = ProtoCommon.recvPackage(storageSocket.getInputStream(), (byte) 100, 0L);
                    this.errno = pkgInfo.errno;
                    if (pkgInfo.errno != 0)
                    {
                        byte var98 = this.errno;
                        return var98;
                    }

                    byte var17 = 0;
                    return var17;
                }

                var16 = this.errno;
            }
            catch (IOException var95)
            {
                if (!bNewConnection)
                {
                    try
                    {
                        this.storageServer.close();
                    }
                    catch (IOException var93)
                    {
                        var93.printStackTrace();
                    }
                    finally
                    {
                        this.storageServer = null;
                    }
                }

                throw var95;
            }
            finally
            {
                if (bNewConnection)
                {
                    try
                    {
                        this.storageServer.close();
                    }
                    catch (IOException var91)
                    {
                        var91.printStackTrace();
                    }
                    finally
                    {
                        this.storageServer = null;
                    }
                }

            }

            return var16;
        }
        else
        {
            this.errno = 22;
            return this.errno;
        }
    }

    protected int do_modify_file(String group_name, String appender_filename, long file_offset, long modify_size, UploadCallback callback, String ip, String port) throws IOException, MyException
    {
        if (group_name != null && group_name.length() != 0 && appender_filename != null && appender_filename.length() != 0)
        {
            boolean bNewConnection = this.newUpdatableStorageConnection(group_name, appender_filename, ip, port);

            byte var19;
            try
            {
                Socket storageSocket = this.storageServer.getSocket();
                byte[] appenderFilenameBytes = appender_filename.getBytes(ClientGlobal.g_charset);
                long body_len = (long) (24 + appenderFilenameBytes.length) + modify_size;
                byte[] header = ProtoCommon.packHeader((byte) 34, body_len, (byte) 0);
                byte[] wholePkg = new byte[(int) ((long) header.length + body_len - modify_size)];
                System.arraycopy(header, 0, wholePkg, 0, header.length);
                int offset = header.length;
                byte[] hexLenBytes = ProtoCommon.long2buff((long) appender_filename.length());
                System.arraycopy(hexLenBytes, 0, wholePkg, offset, hexLenBytes.length);
                offset += hexLenBytes.length;
                hexLenBytes = ProtoCommon.long2buff(file_offset);
                System.arraycopy(hexLenBytes, 0, wholePkg, offset, hexLenBytes.length);
                offset += hexLenBytes.length;
                hexLenBytes = ProtoCommon.long2buff(modify_size);
                System.arraycopy(hexLenBytes, 0, wholePkg, offset, hexLenBytes.length);
                offset += hexLenBytes.length;
                OutputStream out = storageSocket.getOutputStream();
                System.arraycopy(appenderFilenameBytes, 0, wholePkg, offset, appenderFilenameBytes.length);
                int var10000 = offset + appenderFilenameBytes.length;
                out.write(wholePkg);
                if ((this.errno = (byte) callback.send(out)) != 0)
                {
                    byte var99 = this.errno;
                    return var99;
                }

                ProtoCommon.RecvPackageInfo pkgInfo = ProtoCommon.recvPackage(storageSocket.getInputStream(), (byte) 100, 0L);
                this.errno = pkgInfo.errno;
                if (pkgInfo.errno == 0)
                {
                    byte var100 = 0;
                    return var100;
                }

                var19 = this.errno;
            }
            catch (IOException var97)
            {
                if (!bNewConnection)
                {
                    try
                    {
                        this.storageServer.close();
                    }
                    catch (IOException var95)
                    {
                        var95.printStackTrace();
                    }
                    finally
                    {
                        this.storageServer = null;
                    }
                }

                throw var97;
            }
            finally
            {
                if (bNewConnection)
                {
                    try
                    {
                        this.storageServer.close();
                    }
                    catch (IOException var93)
                    {
                        var93.printStackTrace();
                    }
                    finally
                    {
                        this.storageServer = null;
                    }
                }

            }

            return var19;
        }
        else
        {
            this.errno = 22;
            return this.errno;
        }
    }

    public int delete_file(String group_name, String remote_filename) throws IOException, MyException
    {
        String ip = null;
        String port = null;
        boolean bNewConnection = this.newUpdatableStorageConnection(group_name, remote_filename, ip, port);
        Socket storageSocket = this.storageServer.getSocket();

        byte var6;
        try
        {
            this.send_package((byte) 12, group_name, remote_filename);
            ProtoCommon.RecvPackageInfo pkgInfo = ProtoCommon.recvPackage(storageSocket.getInputStream(), (byte) 100, 0L);
            this.errno = pkgInfo.errno;
            var6 = pkgInfo.errno;
        }
        catch (IOException var47)
        {
            if (!bNewConnection)
            {
                try
                {
                    this.storageServer.close();
                }
                catch (IOException var45)
                {
                    var45.printStackTrace();
                }
                finally
                {
                    this.storageServer = null;
                }
            }

            throw var47;
        }
        finally
        {
            if (bNewConnection)
            {
                try
                {
                    this.storageServer.close();
                }
                catch (IOException var43)
                {
                    var43.printStackTrace();
                }
                finally
                {
                    this.storageServer = null;
                }
            }

        }

        return var6;
    }

    public int truncate_file(String group_name, String appender_filename) throws IOException, MyException
    {
        long truncated_file_size = 0L;
        return this.truncate_file(group_name, appender_filename, 0L);
    }

    public int truncate_file(String group_name, String appender_filename, long truncated_file_size) throws IOException, MyException
    {
        if (group_name != null && group_name.length() != 0 && appender_filename != null && appender_filename.length() != 0)
        {
            String ip = null;
            String port = null;
            boolean bNewConnection = this.newUpdatableStorageConnection(group_name, appender_filename, ip, port);

            byte var15;
            try
            {
                Socket storageSocket = this.storageServer.getSocket();
                byte[] appenderFilenameBytes = appender_filename.getBytes(ClientGlobal.g_charset);
                int body_len = 16 + appenderFilenameBytes.length;
                byte[] header = ProtoCommon.packHeader((byte) 36, (long) body_len, (byte) 0);
                byte[] wholePkg = new byte[header.length + body_len];
                System.arraycopy(header, 0, wholePkg, 0, header.length);
                int offset = header.length;
                byte[] hexLenBytes = ProtoCommon.long2buff((long) appender_filename.length());
                System.arraycopy(hexLenBytes, 0, wholePkg, offset, hexLenBytes.length);
                offset += hexLenBytes.length;
                hexLenBytes = ProtoCommon.long2buff(truncated_file_size);
                System.arraycopy(hexLenBytes, 0, wholePkg, offset, hexLenBytes.length);
                offset += hexLenBytes.length;
                OutputStream out = storageSocket.getOutputStream();
                System.arraycopy(appenderFilenameBytes, 0, wholePkg, offset, appenderFilenameBytes.length);
                int var10000 = offset + appenderFilenameBytes.length;
                out.write(wholePkg);
                ProtoCommon.RecvPackageInfo pkgInfo = ProtoCommon.recvPackage(storageSocket.getInputStream(), (byte) 100, 0L);
                this.errno = pkgInfo.errno;
                var15 = pkgInfo.errno;
            }
            catch (IOException var56)
            {
                if (!bNewConnection)
                {
                    try
                    {
                        this.storageServer.close();
                    }
                    catch (IOException var54)
                    {
                        var54.printStackTrace();
                    }
                    finally
                    {
                        this.storageServer = null;
                    }
                }

                throw var56;
            }
            finally
            {
                if (bNewConnection)
                {
                    try
                    {
                        this.storageServer.close();
                    }
                    catch (IOException var52)
                    {
                        var52.printStackTrace();
                    }
                    finally
                    {
                        this.storageServer = null;
                    }
                }

            }

            return var15;
        }
        else
        {
            this.errno = 22;
            return this.errno;
        }
    }

    public byte[] download_file(String group_name, String remote_filename, String ip, String port) throws IOException, MyException
    {
        long file_offset = 0L;
        long download_bytes = 0L;
        return this.download_file(group_name, remote_filename, 0L, 0L, ip, port);
    }

    public byte[] download_file(String group_name, String remote_filename, long file_offset, long download_bytes, String ip, String port) throws IOException, MyException
    {
        boolean bNewConnection = this.newReadableStorageConnection(group_name, remote_filename, ip, port);
        Socket storageSocket = this.storageServer.getSocket();

        Object var10;
        try
        {
            this.send_download_package(group_name, remote_filename, file_offset, download_bytes);
            ProtoCommon.RecvPackageInfo pkgInfo = ProtoCommon.recvPackage(storageSocket.getInputStream(), (byte) 100, -1L);
            this.errno = pkgInfo.errno;
            if (pkgInfo.errno == 0)
            {
                byte[] var70 = pkgInfo.body;
                return var70;
            }

            var10 = null;
        }
        catch (IOException var68)
        {
            if (!bNewConnection)
            {
                try
                {
                    this.storageServer.close();
                }
                catch (IOException var66)
                {
                    var66.printStackTrace();
                }
                finally
                {
                    this.storageServer = null;
                }
            }

            throw var68;
        }
        finally
        {
            if (bNewConnection)
            {
                try
                {
                    this.storageServer.close();
                }
                catch (IOException var64)
                {
                    var64.printStackTrace();
                }
                finally
                {
                    this.storageServer = null;
                }
            }

        }

        return (byte[]) var10;
    }

    public int download_file(String group_name, String remote_filename, String local_filename, String ip, String port) throws IOException, MyException
    {
        long file_offset = 0L;
        long download_bytes = 0L;
        return this.download_file(group_name, remote_filename, 0L, 0L, local_filename, ip, port);
    }

    public int download_file(String group_name, String remote_filename, long file_offset, long download_bytes, String local_filename, String ip, String port) throws IOException, MyException
    {
        boolean bNewConnection = this.newReadableStorageConnection(group_name, remote_filename, ip, port);
        Socket storageSocket = this.storageServer.getSocket();

        try
        {
            FileOutputStream out = new FileOutputStream(local_filename);

            try
            {
                this.errno = 0;
                this.send_download_package(group_name, remote_filename, file_offset, download_bytes);
                InputStream in = storageSocket.getInputStream();
                ProtoCommon.RecvHeaderInfo header = ProtoCommon.recvHeader(in, (byte) 100, -1L);
                this.errno = header.errno;
                byte[] buff;
                if (header.errno != 0)
                {
//          buff = (byte[])header.errno;
//          return (int)buff;
                    return 0;
                }
                else
                {
                    buff = new byte[262144];

                    int bytes;
                    for (long remainBytes = header.body_len; remainBytes > 0L; remainBytes -= (long) bytes)
                    {
                        if ((bytes = in.read(buff, 0, remainBytes > (long) buff.length ? buff.length : (int) remainBytes)) < 0)
                        {
                            throw new IOException("recv package size " + (header.body_len - remainBytes) + " != " + header.body_len);
                        }

                        out.write(buff, 0, bytes);
                    }

                    byte var17 = 0;
                    return var17;
                }
            }
            catch (IOException var93)
            {
                if (this.errno == 0)
                {
                    this.errno = 5;
                }

                throw var93;
            }
            finally
            {
                out.close();
                if (this.errno != 0)
                {
                    (new File(local_filename)).delete();
                }

            }
        }
        catch (IOException var95)
        {
            if (!bNewConnection)
            {
                try
                {
                    this.storageServer.close();
                }
                catch (IOException var91)
                {
                    var91.printStackTrace();
                }
                finally
                {
                    this.storageServer = null;
                }
            }

            throw var95;
        }
        finally
        {
            if (bNewConnection)
            {
                try
                {
                    this.storageServer.close();
                }
                catch (IOException var89)
                {
                    var89.printStackTrace();
                }
                finally
                {
                    this.storageServer = null;
                }
            }

        }
    }

    public int download_file(String group_name, String remote_filename, DownloadCallback callback) throws IOException, MyException
    {
        long file_offset = 0L;
        long download_bytes = 0L;
        String ip = null;
        String port = null;
        return this.download_file(group_name, remote_filename, 0L, 0L, callback, ip, port);
    }

    public int download_file(String group_name, String remote_filename, long file_offset, long download_bytes, DownloadCallback callback, String ip, String port) throws IOException, MyException
    {
        boolean bNewConnection = this.newReadableStorageConnection(group_name, remote_filename, ip, port);
        Socket storageSocket = this.storageServer.getSocket();

        try
        {
            this.send_download_package(group_name, remote_filename, file_offset, download_bytes);
            InputStream in = storageSocket.getInputStream();
            ProtoCommon.RecvHeaderInfo header = ProtoCommon.recvHeader(in, (byte) 100, -1L);
            this.errno = header.errno;
            if (header.errno != 0)
            {
                byte var97 = header.errno;
                return var97;
            }
            else
            {
                byte[] buff = new byte[2048];

                int bytes;
                for (long remainBytes = header.body_len; remainBytes > 0L; remainBytes -= (long) bytes)
                {
                    if ((bytes = in.read(buff, 0, remainBytes > (long) buff.length ? buff.length : (int) remainBytes)) < 0)
                    {
                        throw new IOException("recv package size " + (header.body_len - remainBytes) + " != " + header.body_len);
                    }

                    int result;
                    if ((result = callback.recv(header.body_len, buff, bytes)) != 0)
                    {
                        this.errno = (byte) result;
                        int var17 = result;
                        return var17;
                    }
                }

                byte var98 = 0;
                return var98;
            }
        }
        catch (IOException var95)
        {
            if (!bNewConnection)
            {
                try
                {
                    this.storageServer.close();
                }
                catch (IOException var93)
                {
                    var93.printStackTrace();
                }
                finally
                {
                    this.storageServer = null;
                }
            }

            throw var95;
        }
        finally
        {
            if (bNewConnection)
            {
                try
                {
                    this.storageServer.close();
                }
                catch (IOException var91)
                {
                    var91.printStackTrace();
                }
                finally
                {
                    this.storageServer = null;
                }
            }

        }
    }

    public NameValuePair[] get_metadata(String group_name, String remote_filename) throws IOException, MyException
    {
        String ip = null;
        String port = null;
        boolean bNewConnection = this.newUpdatableStorageConnection(group_name, remote_filename, ip, port);
        Socket storageSocket = this.storageServer.getSocket();

        NameValuePair[] var6;
        try
        {
            this.send_package((byte) 15, group_name, remote_filename);
            ProtoCommon.RecvPackageInfo pkgInfo = ProtoCommon.recvPackage(storageSocket.getInputStream(), (byte) 100, -1L);
            this.errno = pkgInfo.errno;
            if (pkgInfo.errno == 0)
            {
                var6 = ProtoCommon.split_metadata(new String(pkgInfo.body, ClientGlobal.g_charset));
                return var6;
            }

            var6 = null;
        }
        catch (IOException var64)
        {
            if (!bNewConnection)
            {
                try
                {
                    this.storageServer.close();
                }
                catch (IOException var62)
                {
                    var62.printStackTrace();
                }
                finally
                {
                    this.storageServer = null;
                }
            }

            throw var64;
        }
        finally
        {
            if (bNewConnection)
            {
                try
                {
                    this.storageServer.close();
                }
                catch (IOException var60)
                {
                    var60.printStackTrace();
                }
                finally
                {
                    this.storageServer = null;
                }
            }

        }

        return var6;
    }

    public int set_metadata(String group_name, String remote_filename, NameValuePair[] meta_list, byte op_flag) throws IOException, MyException
    {
        String ip = null;
        String port = null;
        boolean bNewConnection = this.newUpdatableStorageConnection(group_name, remote_filename, ip, port);
        Socket storageSocket = this.storageServer.getSocket();

        byte var17;
        try
        {
            byte[] meta_buff;
            if (meta_list == null)
            {
                meta_buff = new byte[0];
            }
            else
            {
                meta_buff = ProtoCommon.pack_metadata(meta_list).getBytes(ClientGlobal.g_charset);
            }

            byte[] filenameBytes = remote_filename.getBytes(ClientGlobal.g_charset);
            byte[] sizeBytes = new byte[16];
            Arrays.fill(sizeBytes, (byte) 0);
            byte[] bs = ProtoCommon.long2buff((long) filenameBytes.length);
            System.arraycopy(bs, 0, sizeBytes, 0, bs.length);
            bs = ProtoCommon.long2buff((long) meta_buff.length);
            System.arraycopy(bs, 0, sizeBytes, 8, bs.length);
            byte[] groupBytes = new byte[16];
            bs = group_name.getBytes(ClientGlobal.g_charset);
            Arrays.fill(groupBytes, (byte) 0);
            int groupLen;
            if (bs.length <= groupBytes.length)
            {
                groupLen = bs.length;
            }
            else
            {
                groupLen = groupBytes.length;
            }

            System.arraycopy(bs, 0, groupBytes, 0, groupLen);
            byte[] header = ProtoCommon.packHeader((byte) 13, (long) (17 + groupBytes.length + filenameBytes.length + meta_buff.length), (byte) 0);
            OutputStream out = storageSocket.getOutputStream();
            byte[] wholePkg = new byte[header.length + sizeBytes.length + 1 + groupBytes.length + filenameBytes.length];
            System.arraycopy(header, 0, wholePkg, 0, header.length);
            System.arraycopy(sizeBytes, 0, wholePkg, header.length, sizeBytes.length);
            wholePkg[header.length + sizeBytes.length] = op_flag;
            System.arraycopy(groupBytes, 0, wholePkg, header.length + sizeBytes.length + 1, groupBytes.length);
            System.arraycopy(filenameBytes, 0, wholePkg, header.length + sizeBytes.length + 1 + groupBytes.length, filenameBytes.length);
            out.write(wholePkg);
            if (meta_buff.length > 0)
            {
                out.write(meta_buff);
            }

            ProtoCommon.RecvPackageInfo pkgInfo = ProtoCommon.recvPackage(storageSocket.getInputStream(), (byte) 100, 0L);
            this.errno = pkgInfo.errno;
            var17 = pkgInfo.errno;
        }
        catch (IOException var58)
        {
            if (!bNewConnection)
            {
                try
                {
                    this.storageServer.close();
                }
                catch (IOException var56)
                {
                    var56.printStackTrace();
                }
                finally
                {
                    this.storageServer = null;
                }
            }

            throw var58;
        }
        finally
        {
            if (bNewConnection)
            {
                try
                {
                    this.storageServer.close();
                }
                catch (IOException var54)
                {
                    var54.printStackTrace();
                }
                finally
                {
                    this.storageServer = null;
                }
            }

        }

        return var17;
    }

    public FileInfo get_file_info(String group_name, String remote_filename) throws IOException, MyException
    {
        if (remote_filename.length() < 44)
        {
            this.errno = 22;
            return null;
        }
        else
        {
            byte[] buff = base64.decodeAuto(remote_filename.substring(10, 37));
            long file_size = ProtoCommon.buff2long(buff, 8);
            FileInfo fileInfo;
            if ((long) remote_filename.length() <= 60L && ((long) remote_filename.length() <= 44L || (file_size & 576460752303423488L) != 0L) && (file_size & 288230376151711744L) == 0L)
            {
                fileInfo = new FileInfo(file_size, 0, 0, ProtoCommon.getIpAddress(buff, 0));
                fileInfo.setCreateTimestamp(ProtoCommon.buff2int(buff, 4));
                if (file_size >> 63 != 0L)
                {
                    file_size &= 4294967295L;
                    fileInfo.setFileSize(file_size);
                }

                fileInfo.setCrc32(ProtoCommon.buff2int(buff, 16));
                return fileInfo;
            }
            else
            {
                fileInfo = this.query_file_info(group_name, remote_filename);
                return fileInfo == null ? null : fileInfo;
            }
        }
    }

    public FileInfo query_file_info(String group_name, String remote_filename) throws IOException, MyException
    {
        String ip = null;
        String port = null;
        boolean bNewConnection = this.newUpdatableStorageConnection(group_name, remote_filename, ip, port);
        Socket storageSocket = this.storageServer.getSocket();

        Object var13;
        try
        {
            byte[] filenameBytes = remote_filename.getBytes(ClientGlobal.g_charset);
            byte[] groupBytes = new byte[16];
            byte[] bs = group_name.getBytes(ClientGlobal.g_charset);
            Arrays.fill(groupBytes, (byte) 0);
            int groupLen;
            if (bs.length <= groupBytes.length)
            {
                groupLen = bs.length;
            }
            else
            {
                groupLen = groupBytes.length;
            }

            System.arraycopy(bs, 0, groupBytes, 0, groupLen);
            byte[] header = ProtoCommon.packHeader((byte) 22, (long) (groupBytes.length + filenameBytes.length), (byte) 0);
            OutputStream out = storageSocket.getOutputStream();
            byte[] wholePkg = new byte[header.length + groupBytes.length + filenameBytes.length];
            System.arraycopy(header, 0, wholePkg, 0, header.length);
            System.arraycopy(groupBytes, 0, wholePkg, header.length, groupBytes.length);
            System.arraycopy(filenameBytes, 0, wholePkg, header.length + groupBytes.length, filenameBytes.length);
            out.write(wholePkg);
            ProtoCommon.RecvPackageInfo pkgInfo = ProtoCommon.recvPackage(storageSocket.getInputStream(), (byte) 100, 40L);
            this.errno = pkgInfo.errno;
            if (pkgInfo.errno == 0)
            {
                long file_size = ProtoCommon.buff2long(pkgInfo.body, 0);
                int create_timestamp = (int) ProtoCommon.buff2long(pkgInfo.body, 8);
                int crc32 = (int) ProtoCommon.buff2long(pkgInfo.body, 16);
                String source_ip_addr = (new String(pkgInfo.body, 24, 16)).trim();
                FileInfo var18 = new FileInfo(file_size, create_timestamp, crc32, source_ip_addr);
                return var18;
            }

            var13 = null;
        }
        catch (IOException var75)
        {
            if (!bNewConnection)
            {
                try
                {
                    this.storageServer.close();
                }
                catch (IOException var73)
                {
                    var73.printStackTrace();
                }
                finally
                {
                    this.storageServer = null;
                }
            }

            throw var75;
        }
        finally
        {
            if (bNewConnection)
            {
                try
                {
                    this.storageServer.close();
                }
                catch (IOException var71)
                {
                    var71.printStackTrace();
                }
                finally
                {
                    this.storageServer = null;
                }
            }

        }

        return (FileInfo) var13;
    }

    protected boolean newWritableStorageConnection(String group_name, String ip, String port) throws IOException, MyException
    {
        if (this.storageServer != null)
        {
            return false;
        }
        else
        {
            MyTrackerClient tracker = new MyTrackerClient();
            this.storageServer = tracker.getStoreStorage(this.trackerServer, group_name, ip, port);
            if (this.storageServer == null)
            {
                throw new MyException("getStoreStorage fail, errno code: " + tracker.getErrorCode());
            }
            else
            {
                return true;
            }
        }
    }

    protected boolean newReadableStorageConnection(String group_name, String remote_filename, String ip, String port) throws IOException, MyException
    {
        if (this.storageServer != null)
        {
            return false;
        }
        else
        {
            MyTrackerClient tracker = new MyTrackerClient();
            this.storageServer = tracker.getFetchStorage(this.trackerServer, group_name, remote_filename, ip, port);
            if (this.storageServer == null)
            {
                throw new MyException("getStoreStorage fail, errno code: " + tracker.getErrorCode());
            }
            else
            {
                return true;
            }
        }
    }

    protected boolean newUpdatableStorageConnection(String group_name, String remote_filename, String ip, String port) throws IOException, MyException
    {
        if (this.storageServer != null)
        {
            return false;
        }
        else
        {
            MyTrackerClient tracker = new MyTrackerClient();
            this.storageServer = tracker.getUpdateStorage(this.trackerServer, group_name, remote_filename, ip, port);
            if (this.storageServer == null)
            {
                throw new MyException("getStoreStorage fail, errno code: " + tracker.getErrorCode());
            }
            else
            {
                return true;
            }
        }
    }

    protected void send_package(byte cmd, String group_name, String remote_filename) throws IOException
    {
        byte[] groupBytes = new byte[16];
        byte[] bs = group_name.getBytes(ClientGlobal.g_charset);
        byte[] filenameBytes = remote_filename.getBytes(ClientGlobal.g_charset);
        Arrays.fill(groupBytes, (byte) 0);
        int groupLen;
        if (bs.length <= groupBytes.length)
        {
            groupLen = bs.length;
        }
        else
        {
            groupLen = groupBytes.length;
        }

        System.arraycopy(bs, 0, groupBytes, 0, groupLen);
        byte[] header = ProtoCommon.packHeader(cmd, (long) (groupBytes.length + filenameBytes.length), (byte) 0);
        byte[] wholePkg = new byte[header.length + groupBytes.length + filenameBytes.length];
        System.arraycopy(header, 0, wholePkg, 0, header.length);
        System.arraycopy(groupBytes, 0, wholePkg, header.length, groupBytes.length);
        System.arraycopy(filenameBytes, 0, wholePkg, header.length + groupBytes.length, filenameBytes.length);
        this.storageServer.getSocket().getOutputStream().write(wholePkg);
    }

    protected void send_download_package(String group_name, String remote_filename, long file_offset, long download_bytes) throws IOException
    {
        byte[] bsOffset = ProtoCommon.long2buff(file_offset);
        byte[] bsDownBytes = ProtoCommon.long2buff(download_bytes);
        byte[] groupBytes = new byte[16];
        byte[] bs = group_name.getBytes(ClientGlobal.g_charset);
        byte[] filenameBytes = remote_filename.getBytes(ClientGlobal.g_charset);
        Arrays.fill(groupBytes, (byte) 0);
        int groupLen;
        if (bs.length <= groupBytes.length)
        {
            groupLen = bs.length;
        }
        else
        {
            groupLen = groupBytes.length;
        }

        System.arraycopy(bs, 0, groupBytes, 0, groupLen);
        byte[] header = ProtoCommon.packHeader((byte) 14, (long) (bsOffset.length + bsDownBytes.length + groupBytes.length + filenameBytes.length), (byte) 0);
        byte[] wholePkg = new byte[header.length + bsOffset.length + bsDownBytes.length + groupBytes.length + filenameBytes.length];
        System.arraycopy(header, 0, wholePkg, 0, header.length);
        System.arraycopy(bsOffset, 0, wholePkg, header.length, bsOffset.length);
        System.arraycopy(bsDownBytes, 0, wholePkg, header.length + bsOffset.length, bsDownBytes.length);
        System.arraycopy(groupBytes, 0, wholePkg, header.length + bsOffset.length + bsDownBytes.length, groupBytes.length);
        System.arraycopy(filenameBytes, 0, wholePkg, header.length + bsOffset.length + bsDownBytes.length + groupBytes.length, filenameBytes.length);
        this.storageServer.getSocket().getOutputStream().write(wholePkg);
    }

    public static class UploadBuff implements UploadCallback
    {
        private byte[] fileBuff;
        private int offset;
        private int length;

        public UploadBuff(byte[] fileBuff, int offset, int length)
        {
            this.fileBuff = fileBuff;
            this.offset = offset;
            this.length = length;
        }

        public int send(OutputStream out) throws IOException
        {
            out.write(this.fileBuff, this.offset, this.length);
            return 0;
        }
    }
}
