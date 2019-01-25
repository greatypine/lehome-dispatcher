package cn.lehome.dispatcher.utils.robot;

import cn.lehome.base.api.content.bean.robot.PostMateriel;
import cn.lehome.base.api.content.service.robot.PostMaterielApiService;
import cn.lehome.base.api.tool.bean.storage.StorageInfo;
import cn.lehome.base.api.tool.compoment.storage.AliyunOSSComponent;
import cn.lehome.bean.tool.entity.enums.storage.StorageObjectType;
import cn.lehome.bean.tool.entity.enums.storage.StorageUsageType;
import cn.lehome.framework.base.api.core.util.ExcelUtils;
import cn.lehome.framework.base.api.core.util.MD5Util;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileFilter;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;

/**
 * Created by wuzhao on 2018/4/2.
 */
@Service("postMaterielService")
public class PostMaterielServiceImpl implements PostMaterielService {

    @Autowired
    private PostMaterielApiService postMaterielApiService;

    @Autowired
    private AliyunOSSComponent aliyunOSSComponent;

    @Override
    public void initPostMateriel(String input[]) {
        String file = input[1];
        int sheetIndex = Integer.valueOf(input[2]);
        File filePath = new File(file);
        for(File f : filePath.listFiles(l->l.getName().endsWith(".xlsx"))){
            try {
                String fileName = f.getName();
                Long typeId = Long.valueOf(fileName.substring(0,fileName.indexOf(".")));
                ExcelUtils excelUtils = new ExcelUtils(f.getAbsolutePath());
                List<List<String>> data = excelUtils.read(sheetIndex);
                Map<Integer, List<byte[]>> picData = excelUtils.readPicture(sheetIndex);
                for (int i = 0; i < data.size(); i++) {
                    List<String> d = data.get(i);
                    if (null == d.get(0) || "".equals(d.get(0).trim())) {
                        continue;
                    }
                    PostMateriel postMateriel = new PostMateriel();
                    postMateriel.setContent(d.get(0));
                    postMateriel.setContentMd5(MD5Util.encoderByMd5(d.get(0)));
                    postMateriel.setTypeId(typeId);
                    List<String> urlList = uploadPic(i, picData);
                    postMateriel.setPicList(urlList);
                    postMaterielApiService.save(postMateriel);
                }
            } catch (Exception e) {
                System.out.println("数据处理出错：" + e.getMessage());
                e.printStackTrace();
                return;
            }
        }
        System.out.println("数据处理完毕");
    }

    private List<String> uploadPic(int i,Map<Integer,List<byte[]>> picData){
        List<byte[]> list = picData.get(i);
        List<String> urlList = new ArrayList<>();
        if(list!=null) {
            int count = 0;
            for (byte[] bytes : list) {
                count++;
                InputStream inputStream = new ByteArrayInputStream(bytes);
                StorageInfo storageInfo = aliyunOSSComponent.putObject(inputStream, "POST" + UUID.randomUUID() + ".png", null, StorageObjectType.CONTENT, StorageUsageType.POST);
                String headUrl = String.format("%s%s", storageInfo.getPrefix(), storageInfo.getRelativeUrl());
                urlList.add(headUrl);
                if(count==9){
                    break;
                }
            }
        }
        return urlList;
    }
}
