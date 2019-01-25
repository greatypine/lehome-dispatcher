package cn.lehome.dispatcher.queue.listener.activity;

import cn.lehome.base.api.activity.bean.advert.AdvertDeliverRange;
import cn.lehome.base.api.activity.bean.redpacket.RedPacketUploadUserBean;
import cn.lehome.base.api.activity.service.advert.AdvertDeliverRangeApiService;
import cn.lehome.bean.activity.enums.advert.DeliverRangeType;
import cn.lehome.dispatcher.queue.listener.AbstractJobListener;
import cn.lehome.dispatcher.queue.service.push.PushService;
import cn.lehome.framework.base.api.core.event.IEventMessage;
import cn.lehome.framework.base.api.core.event.SimpleEventMessage;
import cn.lehome.framework.base.api.core.util.StringUtil;
import org.apache.poi.hssf.usermodel.HSSFWorkbook;
import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.ss.usermodel.Sheet;
import org.apache.poi.ss.usermodel.Workbook;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;
import org.springframework.beans.factory.annotation.Autowired;

import java.io.InputStream;
import java.net.URL;
import java.util.List;

/**
 * @Description 新版业支红包按指定用户投放消费者
 * @author zhuwl
 * @date 2018-07-13
 */
public class RedPacketDeliverUserListener extends AbstractJobListener {

    @Autowired
    private AdvertDeliverRangeApiService advertDeliverRangeApiServiceNew;

    @Autowired
    private PushService pushService;

    @Override
    public void execute(IEventMessage eventMessage) throws Exception {
        SimpleEventMessage<RedPacketUploadUserBean> simpleEventMessage = (SimpleEventMessage<RedPacketUploadUserBean>) eventMessage;
        RedPacketUploadUserBean uploadUserBean = simpleEventMessage.getData();
        logger.error("处理红包上传用户信息 : " + uploadUserBean.toString());

        List<Long> userIds = pushService.readExcelUserInfo(uploadUserBean.getExcelUrl());
        logger.info("一共上传了{}个用户", userIds.size());
        for (Long userId : userIds) {
            AdvertDeliverRange advertDeliverRange = new AdvertDeliverRange();
            advertDeliverRange.setAdvertId(uploadUserBean.getAdvertId());
            advertDeliverRange.setPartnerId(0L);
            advertDeliverRange.setType(DeliverRangeType.USER);
            advertDeliverRange.setTargetId(userId);
            advertDeliverRange.setChargeMode(uploadUserBean.getChargeMode());
            advertDeliverRange.setChargeModeAmount(0L);
            advertDeliverRangeApiServiceNew.save(advertDeliverRange);
        }

    }

    @Override
    public String getConsumerId() {
        return "red_packet_upload_user";
    }

    public static void main(String[] args) throws Exception {
        URL url = new URL("http://dev-lehome-storage-public.oss-cn-beijing.aliyuncs.com/advert/red_packet_deliver_user/f8d53e29-f5e6-44ca-8be4-3bb3657a8036.xlsx");
        InputStream inputStream = url.openConnection().getInputStream();
        String relativePath = url.getFile();
        String ext = relativePath.substring(relativePath.lastIndexOf("."));
        Workbook workbook;
        Sheet sheet;
        Row row;
        if (".xls".equals(ext)) {
            workbook = new HSSFWorkbook(inputStream);
        } else if (".xlsx".equals(ext)) {
            workbook = new XSSFWorkbook(inputStream);
        } else {
            workbook = null;
        }

        // 默认从第一个sheet页读取第一列
        sheet = workbook.getSheetAt(0);
        row = sheet.getRow(0);
        int rowNum = sheet.getLastRowNum();
        for (int i=0; i<rowNum; i++) {
            row = sheet.getRow(i);
            String phone = row.getCell(0).toString();
            if (StringUtil.isBlank(phone)) {
                continue;
            }
            System.out.println(phone);
        }
    }


}
