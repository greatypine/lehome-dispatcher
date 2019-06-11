package cn.lehome.dispatcher.quartz.service.invoke.module;

import cn.lehome.base.api.common.operation.bean.module.subassembly.QSubassemblyDetail;
import cn.lehome.base.api.common.operation.bean.module.subassembly.SubassemblyDetail;
import cn.lehome.base.api.common.operation.service.module.subassembly.SubassemblyDetailApiService;
import cn.lehome.bean.common.operation.enums.module.SubassemblyStatus;
import cn.lehome.dispatcher.quartz.service.AbstractInvokeServiceImpl;
import cn.lehome.framework.base.api.core.request.ApiRequest;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.Date;
import java.util.List;
import java.util.Map;

@Service("subassemblyOnlineScheduleJobService")
public class SubassemblyOnlineScheduleJobServiceImpl extends AbstractInvokeServiceImpl{

    @Autowired
    private SubassemblyDetailApiService subassemblyDetailApiService;

    @Override
    public void doInvoke(Map<String, String> params) {

        ApiRequest apiRequest = ApiRequest.newInstance();

        apiRequest.filterEqual(QSubassemblyDetail.status,SubassemblyStatus.PREPARE_ONLINE);

        apiRequest.filterLessEqual(QSubassemblyDetail.onlineTime,new Date());
        List<SubassemblyDetail> all = subassemblyDetailApiService.findAll(apiRequest);

        for (SubassemblyDetail subassemblyDetail : all){
            subassemblyDetail.setStatus(SubassemblyStatus.ONLINE);
            subassemblyDetailApiService.update(subassemblyDetail);
        }

    }
}
