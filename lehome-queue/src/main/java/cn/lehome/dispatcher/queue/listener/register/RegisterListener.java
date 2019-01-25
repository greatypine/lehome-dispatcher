package cn.lehome.dispatcher.queue.listener.register;

import cn.lehome.base.api.advertising.bean.bonus.BonusItem;
import cn.lehome.base.api.advertising.service.bonus.BonusApiService;
import cn.lehome.base.api.advertising.service.task.UserTaskApiService;
import cn.lehome.base.api.user.bean.user.UserInfo;
import cn.lehome.base.api.user.service.user.UserInfoApiService;
import cn.lehome.bean.advertising.enums.advert.bouns.BonusSourceType;
import cn.lehome.bean.advertising.enums.advert.bouns.BonusType;
import cn.lehome.bean.user.entity.enums.wechat.UserRegisterType;
import cn.lehome.dispatcher.queue.listener.AbstractJobListener;
import cn.lehome.framework.base.api.core.event.IEventMessage;
import cn.lehome.framework.base.api.core.event.StringEventMessage;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;

/**
 * Created by wuzhao on 2018/3/13.
 */
public class RegisterListener extends AbstractJobListener {

    @Autowired
    private UserTaskApiService userTaskApiService;

    @Autowired
    private UserInfoApiService userInfoApiService;

    @Autowired
    private BonusApiService bonusApiService;

    @Value("${first.loginIn.bonus.beans}")
    private Integer beanNum;


    @Override
    public void execute(IEventMessage eventMessage) throws Exception {
        if (!(eventMessage instanceof StringEventMessage)) {
            logger.error("消息类型不对");
            return;
        }
        StringEventMessage stringEventMessage = (StringEventMessage) eventMessage;
        logger.info("register-初始化新手任务数据和礼包, phone={}", stringEventMessage.getData());
        String data = stringEventMessage.getData();
        String[] dataArray = data.split(",");
        userTaskApiService.initTask(dataArray[0]);
        if (dataArray.length == 2 ) {
            //首次登录给奖励
            String versionCode = dataArray[1];
            boolean flag = (versionCode.length() == 3 && versionCode.compareTo("320") >= 0 )
                    || (versionCode.length() == 4 && versionCode.compareTo("3200") >= 0);

            // 如果用户先从微信注册绑定手机号，则不给奖励
            UserInfo userInfo = userInfoApiService.findByPhone(dataArray[0]);
            if (userInfo != null && UserRegisterType.NEWS_SMALL_PROGRAM.equals(userInfo.getRegisterType())) {
                logger.info("已有用户信息，不给新手任务奖励，phone={}", dataArray[0]);
                return;
            }

            BonusItem bonusItem = new BonusItem();
            if (flag) {
                // v3.2 首次登陆赠送2800金豆
                bonusItem.setBonus(beanNum);
                bonusItem.setBonusType(BonusType.BEAN_NUM);
            } else {
                bonusItem.setBonus(1);
                bonusItem.setBonusType(BonusType.DRAW_CARD_NUM);
            }
            bonusItem.setExpireTime(null);
            bonusApiService.save(dataArray[0], BonusSourceType.FIRST_LOGIN, bonusItem);
        }
    }



    @Override
    public String getConsumerId() {
        return "register";
    }
}
