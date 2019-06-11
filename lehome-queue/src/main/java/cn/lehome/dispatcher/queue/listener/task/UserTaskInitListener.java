package cn.lehome.dispatcher.queue.listener.task;

import cn.lehome.base.api.business.activity.service.task.UserTaskApiService;
import cn.lehome.base.api.user.bean.user.UserInfoIndex;
import cn.lehome.base.api.user.service.user.UserInfoIndexApiService;
import cn.lehome.dispatcher.queue.listener.AbstractJobListener;
import cn.lehome.framework.base.api.core.event.IEventMessage;
import cn.lehome.framework.base.api.core.event.StringEventMessage;
import cn.lehome.framework.base.api.core.exception.sql.NotFoundRecordException;
import org.springframework.beans.factory.annotation.Autowired;

/**
 * Created by wuzhao on 2018/3/13.
 */
public class UserTaskInitListener extends AbstractJobListener {

    @Autowired
    private UserTaskApiService userTaskApiService;

    @Autowired
    private UserInfoIndexApiService userInfoIndexApiService;


    @Override
    public void execute(IEventMessage eventMessage) throws Exception {
        if (!(eventMessage instanceof StringEventMessage)) {
            logger.error("消息类型不对");
            return;
        }
        StringEventMessage stringEventMessage = (StringEventMessage) eventMessage;
        UserInfoIndex userInfoIndex = userInfoIndexApiService.findByOpenId(stringEventMessage.getData());
        if (userInfoIndex == null) {
            throw new NotFoundRecordException("未找到用户索引信息");
        }
        logger.info("登录补偿新手任务数据, userOpenId={}", userInfoIndex.getId());
        userTaskApiService.initTask(userInfoIndex.getPhone());
    }



    @Override
    public String getConsumerId() {
        return "login_compensate_user_task";
    }
}
