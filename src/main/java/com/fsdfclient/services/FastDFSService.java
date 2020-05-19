package com.fsdfclient.services;

import com.fsdfclient.client.*;
import com.fsdfclient.myclient.MyFastDFSClient;
import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.PropertySource;
import org.springframework.stereotype.Component;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.multipart.MultipartFile;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.util.Locale;

/**
 * @ClassName FastDFSService
 * @Description //TODO
 * @Date 17:10 2020/5/19
 * @Author lql
 * @version 1.0
 **/
@Component
@PropertySource("classpath:config.properties")
public class FastDFSService
{
    private FastDFSClient fastDFSClient = new FastDFSClient();
    private MyFastDFSClient myFastDFSClient = new MyFastDFSClient();

    @Value("${fastdfs.http_secret_key}")
    private String fastDFSHttpSecretKey;

    @Value("${file_server_addr}")
    private String fileServerAddr;

    @Value("${storage_port}")
    private String storagePort;

    /**
     * 根据指定的路径删除服务器文件，适用于没有保存数据库记录的文件
     *
     * @param filePath
     */
    public FileResponseData deleteFile(String filePath, Locale locale)
    {
        FileResponseData responseData = new FileResponseData();
        try
        {
            fastDFSClient.deleteFile(filePath);
        }
        catch (FastDFSException e)
        {
            e.printStackTrace();
            responseData.setSuccess(false);
            responseData.setCode(e.getCode());
            responseData.setMessage(e.getMessage());
        }
        return responseData;
    }

    /**
     * 以附件形式下载文件
     *
     * @param filePath 文件地址
     * @param response
     */
    public void downloadFile(String filePath, HttpServletResponse response) throws FastDFSException
    {
        try
        {
            fastDFSClient.downloadFile(filePath, response);
        }
        catch (FastDFSException e)
        {
            e.printStackTrace();
            throw e;
        }
    }

    /**
     * 获取图片 使用输出流输出字节码，可以使用< img>标签显示图片<br>
     *
     * @param filePath 图片地址
     * @param response
     */
    public void downloadImage(String filePath, HttpServletResponse response) throws FastDFSException
    {
        try
        {
            fastDFSClient.downloadFile(filePath, response.getOutputStream());
        }
        catch (FastDFSException e)
        {
            e.printStackTrace();
            throw e;
        }
        catch (IOException e)
        {
            e.printStackTrace();
        }
    }

    /**
     * 获取访问文件的token
     *
     * @param filePath 文件路径
     * @return
     */
    public FileResponseData getToken(String filePath)
    {
        FileResponseData responseData = new FileResponseData();
        // 设置访文件的Http地址. 有时效性.
        String token = FastDFSClient.getToken(filePath, fastDFSHttpSecretKey);
        responseData.setToken(token);
        responseData.setHttpUrl(fileServerAddr + "/" + filePath + "?" + token);

        return responseData;
    }

    /**
     * 只能上传文档，只上传文件到服务器，不会保存记录到数据库. <br>
     * 会检查文件格式是否正确，默认只能上传 ['pdf', 'ppt', 'xls', 'xlsx', 'pptx', 'doc', 'docx'] 几种类型.
     *
     * @param file
     * @param request
     * @return 返回文件路径等信息
     */
    public FileResponseData uploadDocSample(@RequestParam MultipartFile file, HttpServletRequest request)
    {
        // 检查文件类型
        if (!FileCheck.checkDoc(file.getOriginalFilename()))
        {
            FileResponseData responseData = new FileResponseData(false);
            responseData.setCode(ErrorCode.FILE_TYPE_ERROR_DOC.CODE);
            responseData.setMessage(ErrorCode.FILE_TYPE_ERROR_DOC.MESSAGE);
            return responseData;
        }

        return uploadSampleWithoutToken(file, request);
    }

    public FileResponseData uploadFileSample(MultipartFile file, HttpServletRequest request)
    {
        return uploadSampleWithoutToken(file, request);
    }

    /**
     * 上传通用方法，不要token
     *
     * @param file
     * @param request
     * @return
     */
    public FileResponseData uploadSampleWithoutToken(MultipartFile file, HttpServletRequest request)
    {
        return uploadSample(file, request, false);
    }

    /**
     * 上传通用方法，只上传到服务器，不保存记录到数据库
     *
     * @param file
     * @param request
     * @return
     */
    public FileResponseData uploadSample(MultipartFile file, HttpServletRequest request, boolean withToken)
    {
        String ip = fileServerAddr;
        String port = storagePort;
        String filepath;
        FileResponseData responseData = new FileResponseData();
        try
        {
            // 上传到服务器
            if (StringUtils.isEmpty(port))
            {
                filepath = fastDFSClient.uploadFileWithMultipart(file);
            }
            else
            {
                filepath = myFastDFSClient.uploadFileWithMultipart(file, ip, port);
            }

            responseData.setFileName(file.getOriginalFilename());
            responseData.setFilePath(filepath);
            responseData.setFileType(FastDFSClient.getFilenameSuffix(file.getOriginalFilename()));
            // 设置访文件的Http地址. 有时效性.
            if (withToken)
            {
                String token = FastDFSClient.getToken(filepath, fastDFSHttpSecretKey);
                responseData.setToken(token);
                responseData.setHttpUrl(fileServerAddr + "/" + filepath + "?" + token);
            }
            else
            {
                responseData.setHttpUrl(fileServerAddr + "/" + filepath);
            }
        }
        catch (FastDFSException e)
        {
            responseData.setSuccess(false);
            responseData.setCode(e.getCode());
            responseData.setMessage(e.getMessage());
        }

        return responseData;
    }

    /**
     * 只能上传图片，只上传文件到服务器，不会保存记录到数据库. <br>
     * 会检查文件格式是否正确，默认只能上传 ['png', 'gif', 'jpeg', 'jpg'] 几种类型.
     *
     * @param file
     * @param request
     * @return 返回文件路径等信息
     */
    public FileResponseData uploadImageSample(@RequestParam MultipartFile file, HttpServletRequest request)
    {
        // 检查文件类型
        if (!FileCheck.checkImage(file.getOriginalFilename()))
        {
            FileResponseData responseData = new FileResponseData(false);
            responseData.setCode(ErrorCode.FILE_TYPE_ERROR_IMAGE.CODE);
            responseData.setMessage(ErrorCode.FILE_TYPE_ERROR_IMAGE.MESSAGE);
            return responseData;
        }

        return uploadSampleWithoutToken(file, request);
    }
}
