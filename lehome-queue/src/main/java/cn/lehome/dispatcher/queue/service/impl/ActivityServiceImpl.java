package cn.lehome.dispatcher.queue.service.impl;

import cn.lehome.base.api.advertising.service.activity.MasterApprenticeRelationshipApiService;
import cn.lehome.base.api.tool.util.DateUtil;
import cn.lehome.dispatcher.queue.service.activity.ActivityService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.redis.core.HashOperations;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Map;
import java.util.concurrent.TimeUnit;

@Service
public class ActivityServiceImpl implements ActivityService {

    @Autowired
    private MasterApprenticeRelationshipApiService masterApprenticeRelationshipApiService;

    @Autowired
    private StringRedisTemplate stringRedisTemplate;

    private Logger logger = LoggerFactory.getLogger(this.getClass());

    @Override
    public Integer getLastInviteInfoByMasterId(Long masterId, String operDate) {
        String key = String.format("%s%s_%s", MASTER_APPRENTICE_CACHE_KEY, masterId, operDate);
        Integer apprenticeCount = 0;
        HashOperations<String, String, String> hashOperations = stringRedisTemplate.opsForHash();
        if (stringRedisTemplate.hasKey(key)) {
            Map<String, String> data = hashOperations.entries(key);
            apprenticeCount = Integer.valueOf(data.get(INVITE_COUNT));
        }
        return apprenticeCount;
    }

    @Override
    public void addOneNewApprentice(Long masterId, String operDate) {
        String key = String.format("%s%s_%s", MASTER_APPRENTICE_CACHE_KEY, masterId, operDate);
        HashOperations<String, String, String> hashOperations = stringRedisTemplate.opsForHash();

        hashOperations.increment(key, INVITE_COUNT, 1);
        stringRedisTemplate.expire(key, 1, TimeUnit.DAYS);
    }

    /**
     * 需要注意数据库中无邀请记录情况，即apprenticeId = 0
     */
    private Integer getMasterInviteInfoFromDB(Long masterId, String now) {
        Date startTime = new Date();
        try {
            startTime = new SimpleDateFormat("yyyy-MM-dd").parse(now);
        } catch (ParseException e) {
            e.printStackTrace();
        }
        Date endTime = DateUtil.addDays(startTime, 1);
        return masterApprenticeRelationshipApiService.findInviteApprenticeNumBetweenTime(masterId, startTime, endTime);
    }
}
