package cn.lehome.dispatcher.queue.listener.dataExport;

import cn.lehome.base.api.common.bean.dataexport.DataExportRecord;
import cn.lehome.base.api.common.component.jms.EventBusComponent;
import cn.lehome.base.api.common.constant.EventConstants;
import cn.lehome.base.api.common.event.ExportDataEventBean;
import cn.lehome.base.api.common.service.dataexport.DataExportRecordApiService;
import cn.lehome.dispatcher.queue.enums.DataExportServiceEnum;
import cn.lehome.dispatcher.queue.listener.AbstractJobListener;
import cn.lehome.dispatcher.queue.service.dataExport.DataExportService;
import cn.lehome.framework.base.api.core.compoment.context.SpringContextHolder;
import cn.lehome.framework.base.api.core.event.IEventMessage;
import cn.lehome.framework.base.api.core.event.SimpleEventMessage;
import com.sun.javafx.binding.StringFormatter;
import org.springframework.beans.factory.annotation.Autowired;

/**
 * Created by zuoguodong on 2019/9/26
 */
public class ExportDataListener extends AbstractJobListener {

    @Autowired
    DataExportRecordApiService dataExportRecordApiService;

    @Autowired
    EventBusComponent eventBusComponent;

    @Override
    public void execute(IEventMessage eventMessage) throws Exception {
        if (!(eventMessage instanceof SimpleEventMessage)) {
            logger.error("消息类型不对");
            return;
        }
        SimpleEventMessage<ExportDataEventBean> simpleEventMessage = (SimpleEventMessage<ExportDataEventBean>)eventMessage;
        Long recordId = simpleEventMessage.getData().getRecordId();
        Long pageIndex = simpleEventMessage.getData().getPageNo();
        DataExportRecord dataExportRecord  = dataExportRecordApiService.findOne(recordId);
        if(dataExportRecord == null){
            logger.error("没有找到导出记录");
            return;
        }
        DataExportService dataExportService = SpringContextHolder.getBean(DataExportServiceEnum.getServiceName(dataExportRecord.getBusinessKey()),DataExportService.class);
        dataExportRecord = dataExportService.exportData(dataExportRecord,pageIndex);
        logger.info(String.format("成功导出%s数据第%s页%s条",dataExportRecord.getBusinessKey(),pageIndex,dataExportRecord.getExportRecordNum()));
        dataExportRecord = dataExportRecordApiService.update(dataExportRecord);
        if(dataExportRecord.getTotalRecordNum() > dataExportRecord.getExportRecordNum()){
            pageIndex++;
            ExportDataEventBean exportDataEventBean = new ExportDataEventBean();
            exportDataEventBean.setRecordId(recordId);
            exportDataEventBean.setPageNo(pageIndex);
            eventBusComponent.sendEventMessage(new SimpleEventMessage<>(EventConstants.EXPORT_DATA_EVENT, exportDataEventBean));
        }else{
            logger.info(String.format("%s数据导出完成,共%s条,文件路径%s",dataExportRecord.getBusinessKey(),dataExportRecord.getExportRecordNum(),dataExportRecord.getPrefix() + dataExportRecord.getRelativeUrl()));
        }
    }

    @Override
    public String getConsumerId() {
        return "data_export";
    }
}
