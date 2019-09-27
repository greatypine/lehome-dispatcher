package cn.lehome.dispatcher.queue.service.dataExport;

import cn.lehome.base.api.common.bean.dataexport.DataExportRecord;

/**
 * Created by zuoguodong on 2019/9/26
 */
public interface DataExportService {

    DataExportRecord exportData(DataExportRecord dataExportRecord, Long pageIndex);

}
