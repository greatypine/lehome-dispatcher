package cn.lehome.dispatcher.quartz.service.invoke.post;

import cn.lehome.base.api.business.content.bean.post.PostInfo;
import cn.lehome.base.api.business.content.bean.post.QPostInfo;
import cn.lehome.base.api.business.content.service.post.PostInfoApiService;
import cn.lehome.dispatcher.quartz.service.AbstractInvokeServiceImpl;
import cn.lehome.framework.base.api.core.request.ApiRequest;
import cn.lehome.framework.bean.core.enums.YesNoStatus;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.Date;
import java.util.List;
import java.util.Map;

/**
 * 定时更新帖子置顶状态为未置顶
 * Created by zhanghuan on 2018/10/15.
 */
@Service("cancelPostTopStatusService")
public class CancelPostTopStatusServiceImpl extends AbstractInvokeServiceImpl {

    @Autowired
    private PostInfoApiService postInfoApiService;

    @Override
    public void doInvoke(Map<String, String> params) {
        ApiRequest apiRequest = ApiRequest.newInstance();

        apiRequest.filterEqual(QPostInfo.topStatus,YesNoStatus.YES);

        apiRequest.filterLessEqual(QPostInfo.cancellationTime,new Date());
        List<PostInfo> all = postInfoApiService.findAll(apiRequest);

        for (PostInfo postInfo : all){
            postInfoApiService.updateTopStatus(postInfo.getPostId(),YesNoStatus.NO);
        }
    }
}
