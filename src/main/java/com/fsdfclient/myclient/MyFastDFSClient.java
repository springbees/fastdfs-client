package com.fsdfclient.myclient;

import com.fsdfclient.client.ErrorCode;
import com.fsdfclient.client.FastDFSClient;
import com.fsdfclient.client.FastDFSException;
import com.fsdfclient.client.TrackerServerPool;
import org.apache.commons.codec.binary.Base64;
import org.apache.commons.lang3.StringUtils;
import org.csource.common.MyException;
import org.csource.common.NameValuePair;
import org.csource.fastdfs.TrackerServer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.web.multipart.MultipartFile;

import java.io.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * @ClassName MyFastDFSClient
 * @Description //TODO
 * @Date 17:11 2020/5/19
 * @Author lql
 * @version 1.0
 **/
public class MyFastDFSClient extends FastDFSClient
{
    /**
     * org.slf4j.Logger
     */
    private static Logger logger = LoggerFactory.getLogger(FastDFSClient.class);
    /**
     * 文件名称Key
     */
    private static final String FILENAME = "filename";
    /**
     * 文件最大的大小
     */
    private int maxFileSize = 100 * 1000 * 1000;

    /**
     * MultipartFile 上传文件
     *
     * @param file MultipartFile
     * @return 返回上传成功后的文件路径
     */
    public String uploadFileWithMultipart(MultipartFile file, String ip, String port) throws FastDFSException
    {
        return upload(file, null, ip, port);
    }

    /**
     * MultipartFile 上传文件
     *
     * @param file         MultipartFile
     * @param descriptions 文件描述
     * @return 返回上传成功后的文件路径
     */
    public String uploadFileWithMultipart(MultipartFile file, Map<String, String> descriptions, String ip, String port) throws FastDFSException
    {
        return upload(file, descriptions, ip, port);
    }

    /**
     * 根据指定的路径上传文件
     *
     * @param filepath 文件路径
     * @return 返回上传成功后的文件路径
     */
    public String uploadFileWithFilepath(String filepath, String ip, String port) throws FastDFSException
    {
        return upload(filepath, null, ip, port);
    }

    /**
     * 根据指定的路径上传文件
     *
     * @param filepath     文件路径
     * @param descriptions 文件描述
     * @return 返回上传成功后的文件路径
     */
    public String uploadFileWithFilepath(String filepath, Map<String, String> descriptions, String ip, String port) throws FastDFSException
    {
        return upload(filepath, descriptions, ip, port);
    }

    /**
     * 上传base64文件
     *
     * @param base64 文件base64
     * @return 返回上传成功后的文件路径
     */
    public String uploadFileWithBase64(String base64, String ip, String port) throws FastDFSException
    {
        return upload(base64, null, null, ip, port);
    }

    /**
     * 上传base64文件
     *
     * @param base64   文件base64
     * @param filename 文件名
     * @return 返回上传成功后的文件路径
     */
    public String uploadFileWithBase64(String base64, String filename, String ip, String port) throws FastDFSException
    {
        return upload(base64, filename, null, ip, port);
    }

    /**
     * 上传base64文件
     *
     * @param base64       文件base64
     * @param filename     文件名
     * @param descriptions 文件描述信息
     * @return 返回上传成功后的文件路径
     */
    public String uploadFileWithBase64(String base64, String filename, Map<String, String> descriptions, String ip, String port) throws FastDFSException
    {
        return upload(base64, filename, descriptions, ip, port);
    }

    /**
     * 使用 MultipartFile 上传
     *
     * @param file         MultipartFile
     * @param descriptions 文件描述信息
     * @return 文件路径
     * @throws FastDFSException file为空则抛出异常
     */
    public String upload(MultipartFile file, Map<String, String> descriptions, String ip, String port) throws FastDFSException
    {
        if (file == null || file.isEmpty())
        {
            throw new FastDFSException(ErrorCode.FILE_ISNULL.CODE, ErrorCode.FILE_ISNULL.MESSAGE);
        }
        String path = null;
        try
        {
            path = upload(file.getInputStream(), file.getOriginalFilename(), descriptions, ip, port);
        }
        catch (IOException e)
        {
            e.printStackTrace();
            throw new FastDFSException(ErrorCode.FILE_ISNULL.CODE, ErrorCode.FILE_ISNULL.MESSAGE);
        }
        return path;
    }

    /**
     * 根据指定的路径上传
     *
     * @param filepath     文件路径
     * @param descriptions 文件描述
     * @return 文件路径
     * @throws FastDFSException 文件路径为空则抛出异常
     */
    public String upload(String filepath, Map<String, String> descriptions, String ip, String port) throws FastDFSException
    {
        if (StringUtils.isBlank(filepath))
        {
            throw new FastDFSException(ErrorCode.FILE_PATH_ISNULL.CODE, ErrorCode.FILE_PATH_ISNULL.MESSAGE);
        }
        File file = new File(filepath);
        String path = null;
        try
        {
            InputStream is = new FileInputStream(file);
            // 获取文件名
            filepath = toLocal(filepath);
            String filename = filepath.substring(filepath.lastIndexOf("/") + 1);

            path = upload(is, filename, descriptions, ip, port);
        }
        catch (FileNotFoundException e)
        {
            e.printStackTrace();
            throw new FastDFSException(ErrorCode.FILE_NOT_EXIST.CODE, ErrorCode.FILE_NOT_EXIST.MESSAGE);
        }

        return path;
    }

    /**
     * 上传base64文件
     *
     * @param base64
     * @param filename     文件名
     * @param descriptions 文件描述信息
     * @return 文件路径
     * @throws FastDFSException base64为空则抛出异常
     */
    public String upload(String base64, String filename, Map<String, String> descriptions, String ip, String port) throws FastDFSException
    {
        if (StringUtils.isBlank(base64))
        {
            throw new FastDFSException(ErrorCode.FILE_ISNULL.CODE, ErrorCode.FILE_ISNULL.MESSAGE);
        }
        return upload(new ByteArrayInputStream(Base64.decodeBase64(base64)), filename, descriptions, ip, port);
    }

    /**
     * 上传通用方法
     *
     * @param is           文件输入流
     * @param filename     文件名
     * @param descriptions 文件描述信息
     * @return 组名+文件路径，如：group1/M00/00/00/wKgz6lnduTeAMdrcAAEoRmXZPp870.jpeg
     * @throws FastDFSException
     */
    public String upload(InputStream is, String filename, Map<String, String> descriptions, String ip, String port) throws FastDFSException
    {
        if (is == null)
        {
            throw new FastDFSException(ErrorCode.FILE_ISNULL.CODE, ErrorCode.FILE_ISNULL.MESSAGE);
        }

        try
        {
            if (is.available() > maxFileSize)
            {
                throw new FastDFSException(ErrorCode.FILE_OUT_SIZE.CODE, ErrorCode.FILE_OUT_SIZE.MESSAGE);
            }
        }
        catch (IOException e)
        {
            e.printStackTrace();
        }

        filename = toLocal(filename);
        // 返回路径
        String path = null;
        // 文件描述
        NameValuePair[] nvps = null;
        List<NameValuePair> nvpsList = new ArrayList<>();
        // 文件名后缀
        String suffix = getFilenameSuffix(filename);

        // 文件名
        if (StringUtils.isNotBlank(filename))
        {
            nvpsList.add(new NameValuePair(FILENAME, filename));
        }
        // 描述信息
        if (descriptions != null && descriptions.size() > 0)
        {
            descriptions.forEach((key, value) -> {
                nvpsList.add(new NameValuePair(key, value));
            });
        }
        if (nvpsList.size() > 0)
        {
            nvps = new NameValuePair[nvpsList.size()];
            nvpsList.toArray(nvps);
        }

        TrackerServer trackerServer = TrackerServerPool.borrowObject();
        StorageClient2 storageClient = new StorageClient2(trackerServer, null);
        try
        {
            // 读取流
            byte[] fileBuff = new byte[is.available()];
            is.read(fileBuff, 0, fileBuff.length);

            // 上传
            path = storageClient.upload_file2(fileBuff, suffix, nvps, ip, port);

            if (StringUtils.isBlank(path))
            {
                throw new FastDFSException(ErrorCode.FILE_UPLOAD_FAILED.CODE, ErrorCode.FILE_UPLOAD_FAILED.MESSAGE);
            }

            if (logger.isDebugEnabled())
            {
                logger.debug("upload file success, return path is {}", path);
            }
        }
        catch (IOException e)
        {
            e.printStackTrace();
            throw new FastDFSException(ErrorCode.FILE_UPLOAD_FAILED.CODE, ErrorCode.FILE_UPLOAD_FAILED.MESSAGE);
        }
        catch (MyException e)
        {
            e.printStackTrace();
            throw new FastDFSException(ErrorCode.FILE_UPLOAD_FAILED.CODE, ErrorCode.FILE_UPLOAD_FAILED.MESSAGE);
        }
        finally
        {
            // 关闭流
            if (is != null)
            {
                try
                {
                    is.close();
                }
                catch (IOException e)
                {
                    e.printStackTrace();
                }
            }
        }
        // 返还对象
        TrackerServerPool.returnObject(trackerServer);

        return path;
    }
}
