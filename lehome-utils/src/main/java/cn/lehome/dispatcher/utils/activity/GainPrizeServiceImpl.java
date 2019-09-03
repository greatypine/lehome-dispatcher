package cn.lehome.dispatcher.utils.activity;

import cn.lehome.base.api.user.bean.asset.UserBeanFlowInfo;
import cn.lehome.base.api.user.service.asset.UserAssetApiService;
import cn.lehome.bean.advertising.enums.task.AssetType;
import cn.lehome.framework.base.api.core.util.ExcelUtils;
import cn.lehome.framework.bean.core.enums.Operation;
import cn.lehome.framework.bean.core.enums.OperationType;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.io.File;
import java.util.Date;
import java.util.List;

/**
 * Created by zuoguodong on 2019/5/13
 */
@Service
public class GainPrizeServiceImpl implements GainPrizeService {

    @Autowired
    private UserAssetApiService userAssetApiService;

    @Override
    public void gainPrize(String[] input) {
        String filePath = input[1];
        int sheetIndex = 0;
        if (input.length < 2) {
            System.out.println("参数不正确");
            return;
        }
        File file = new File(filePath);
        if (!file.isFile()) {
            System.out.println("文件路径不对，非法输入");
            return;
        }
        ExcelUtils excelUtils = new ExcelUtils(file.getAbsolutePath());
        List<List<String>> list = excelUtils.read(sheetIndex);
        list.stream().forEach( obj -> {
            UserBeanFlowInfo userBeanFlowInfo = new UserBeanFlowInfo();
            userBeanFlowInfo.setUserId(Long.valueOf(obj.get(1)));
            userBeanFlowInfo.setOperation(Operation.ADD);
            userBeanFlowInfo.setOperationNum(Long.valueOf(obj.get(2)));
            userBeanFlowInfo.setOperationType(OperationType.COLLECT_GAIN_PRIZE);
            userBeanFlowInfo.setOperationTime(new Date());
            userAssetApiService.operateBeanNum(userBeanFlowInfo);
        });
        System.out.println("金豆数据刷新完成");
    }

}
