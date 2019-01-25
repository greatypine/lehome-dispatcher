package cn.lehome.dispatcher.utils.user;

import cn.lehome.base.api.user.bean.message.QUserMessageIndex;
import cn.lehome.base.api.user.bean.message.UserMessage;
import cn.lehome.base.api.user.service.message.UserMessageApiService;
import cn.lehome.bean.user.search.message.UserMessageIndexEntity;
import cn.lehome.dispatcher.utils.es.util.EsFlushUtil;
import cn.lehome.framework.base.api.core.enums.PageOrderType;
import cn.lehome.framework.base.api.core.request.ApiRequest;
import cn.lehome.framework.base.api.core.request.ApiRequestPage;
import cn.lehome.framework.base.api.core.response.ApiResponse;
import cn.lehome.framework.base.api.core.util.BeanMapping;
import com.google.common.collect.Lists;
import org.apache.commons.collections.CollectionUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.util.List;

/**
 * Created by zhanghuan on 2018/7/17.
 */
@Service
public class UserMessageServiceImpl implements UserMessageService{

    @Autowired
    private UserMessageApiService userMessageApiService;

    @Override
    public void flushUserMessageIndexInfo() {

        ApiRequest apiRequest = ApiRequest.newInstance();
        ApiRequestPage apiRequestPage = ApiRequestPage.newInstance();
        int pageIndex = 0;
        int pageSize = 1;
        ApiResponse<UserMessage> apiResponse = userMessageApiService.findAll(apiRequest, apiRequestPage.paging(pageIndex, pageSize).addOrder(QUserMessageIndex.id,PageOrderType.DESC));
        //List<UserMessage> userMessageList = Lists.newArrayList(apiResponse.getPagedData());
        List<UserMessageIndexEntity> userMessageIndexEntityList = BeanMapping.mapList(Lists.newArrayList(apiResponse.getPagedData()),UserMessageIndexEntity.class);
        EsFlushUtil.getInstance().batchInsert(userMessageIndexEntityList);
       /* if (CollectionUtils.isNotEmpty(apiResponse.getPagedData())){
            long totalPage = apiResponse.getTotal() % pageSize == 0 ? apiResponse.getTotal() / pageSize : apiResponse.getTotal() / pageSize + 1;
            System.out.println("总共需要处理【"+ totalPage + "】页");
            while (pageIndex < totalPage) {
                System.out.println("当前处理页："+ pageIndex);
                List<UserMessageIndexEntity> userMessageIndexEntityList = BeanMapping.mapList(Lists.newArrayList(apiResponse.getPagedData()),UserMessageIndexEntity.class);
                EsFlushUtil.getInstance().batchInsert(userMessageIndexEntityList);
                pageIndex++;
                apiResponse = userMessageApiService.findBaseStaticAll(apiRequest, apiRequestPage.paging(pageIndex, pageSize));
            }
        }*/
    }
}
