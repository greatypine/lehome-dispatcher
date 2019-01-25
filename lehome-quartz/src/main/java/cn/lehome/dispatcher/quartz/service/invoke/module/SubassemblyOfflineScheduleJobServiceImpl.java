package cn.lehome.dispatcher.quartz.service.invoke.module;

import cn.lehome.base.api.operation.bean.module.subassembly.QSubassemblyDetail;
import cn.lehome.base.api.operation.bean.module.subassembly.SubassemblyDetail;
import cn.lehome.base.api.operation.service.module.subassembly.SubassemblyDetailApiService;
import cn.lehome.bean.operation.entity.enums.module.SubassemblyStatus;
import cn.lehome.dispatcher.quartz.service.AbstractInvokeServiceImpl;
import cn.lehome.framework.base.api.core.request.ApiRequest;
import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.Date;
import java.util.List;
import java.util.Map;

@Service("subassemblyOfflineScheduleJobService")
public class SubassemblyOfflineScheduleJobServiceImpl extends AbstractInvokeServiceImpl{

    @Autowired
    private SubassemblyDetailApiService subassemblyDetailApiService;

    @Override
    public void doInvoke(Map<String, String> params) {
        ApiRequest apiRequest = ApiRequest.newInstance();

        apiRequest.filterEqual(QSubassemblyDetail.status,SubassemblyStatus.ONLINE);

        apiRequest.filterLessEqual(QSubassemblyDetail.offlineTime,new Date());
        List<SubassemblyDetail> all = subassemblyDetailApiService.findAll(apiRequest);

        for (SubassemblyDetail subassemblyDetail : all){
            subassemblyDetail.setStatus(SubassemblyStatus.OFFLINE);
            subassemblyDetailApiService.update(subassemblyDetail);
        }
    }
}
