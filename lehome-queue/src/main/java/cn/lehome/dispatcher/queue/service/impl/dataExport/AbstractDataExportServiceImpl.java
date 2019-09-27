package cn.lehome.dispatcher.queue.service.impl.dataExport;

import cn.lehome.base.api.common.bean.dataexport.DataExportRecord;
import cn.lehome.base.api.common.bean.storage.StorageInfo;
import cn.lehome.base.api.common.component.storage.AliyunOSSComponent;
import cn.lehome.bean.common.enums.storage.StorageObjectType;
import cn.lehome.bean.common.enums.storage.StorageUsageType;
import cn.lehome.dispatcher.queue.service.dataExport.DataExportService;
import com.alibaba.druid.util.StringUtils;
import org.apache.poi.hssf.usermodel.HSSFWorkbook;
import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.ss.usermodel.Sheet;
import org.apache.poi.ss.usermodel.Workbook;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;

import java.io.*;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;

/**
 * Created by zuoguodong on 2019/9/26
 */
public abstract class AbstractDataExportServiceImpl implements DataExportService {

    protected Logger logger = LoggerFactory.getLogger(getClass());

    private final static String LOCAL_PATH = "temp/download/";

    @Autowired
    private AliyunOSSComponent aliyunOSSComponent;

    private String downloadOssFile(String fileUrl){
        StorageInfo storageInfo = new StorageInfo();
        storageInfo.setObjectType(StorageObjectType.SMART_COMMUNITY);
        storageInfo.setUsageType(StorageUsageType.SMART_EXCEL);
        storageInfo.setRelativeUrl(fileUrl);
        InputStream is = aliyunOSSComponent.getObject(storageInfo);
        if(is == null){
            logger.error("%s文件不存在,无法下载",fileUrl);
            return LOCAL_PATH + fileUrl;
        }
        FileOutputStream fos = null;
        try {
            File file = new File(LOCAL_PATH + fileUrl);
            if(!file.getParentFile().exists()){
                file.getParentFile().mkdirs();
            }
            fos = new FileOutputStream(file);
            byte[] array = new byte[1024];
            int len;
            while((len = is.read(array)) != -1){
                fos.write(array,0,len);
            }
        }catch(IOException e){
            logger.error("下载文件时出错",e);
        }finally{
            try {
                is.close();
                if(fos != null){
                    fos.close();
                }
            }catch(IOException e){
                logger.error("下载文件关闭流时出错",e);
            }
        }
        return LOCAL_PATH + fileUrl;
    }

    private StorageInfo uploadOssFile(InputStream is,String file){
        return aliyunOSSComponent.putObject(is,file,null,StorageObjectType.SMART_COMMUNITY,StorageUsageType.SMART_EXCEL);
    }

    private String getTimestamp(){
        return new SimpleDateFormat("yyyyMMddHHmmss").format(new Date());
    }

    protected void appendExcelData(DataExportRecord dataExportRecord, List<List<String>> rowList){
        Workbook wb;
        String fileName;
        try {
            if(!StringUtils.isEmpty(dataExportRecord.getRelativeUrl())){
                String filePath = this.downloadOssFile(dataExportRecord.getRelativeUrl());
                File file = new File(filePath);
                if(!file.getParentFile().exists()){
                    file.getParentFile().mkdirs();
                }
                InputStream is = new FileInputStream(filePath);
                wb = new HSSFWorkbook(is);
                Sheet sheet = wb.getSheetAt(0);
                int lastRowIndex = sheet.getLastRowNum();
                addData(sheet,rowList,lastRowIndex+1);
                fileName = new File(filePath).getName();
            }else{
                fileName = this.getFileName() + this.getTimestamp() + ".xls";
                wb = new HSSFWorkbook();
                Sheet sheet = wb.createSheet();
                addTitle(sheet);
                addData(sheet, rowList, 1);
            }
            ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
            wb.write(outputStream);
            InputStream inputstream = new ByteArrayInputStream(outputStream.toByteArray());
            StorageInfo storageInfo = this.uploadOssFile(inputstream,fileName);
            dataExportRecord.setPrefix(storageInfo.getPrefix());
            dataExportRecord.setRelativeUrl(storageInfo.getRelativeUrl());
        }catch(Exception e){
            logger.error("写excel时出错",e);
        }
        dataExportRecord.addExportRecordNum(rowList.size());
    }

    private void addTitle(Sheet sheet){
        String[] title = this.getTitle();
        Row row = sheet.createRow(0);
        for(int i = 0;i<title.length;i++){
            row.createCell(i).setCellValue(title[i]);
        }
    }

    private void addData(Sheet sheet,List<List<String>> rowList,int startIndex){
        for (List<String> rd : rowList) {
            Row row = sheet.createRow(startIndex);
            for(int i = 0;i<rd.size();i++){
                if(StringUtils.isEmpty(rd.get(i))){
                    row.createCell(i).setCellValue("");
                }else{
                    row.createCell(i).setCellValue(rd.get(i));
                }
            }
            startIndex++;
        }
    }

    public abstract String[] getTitle();

    public abstract String getFileName();

}
