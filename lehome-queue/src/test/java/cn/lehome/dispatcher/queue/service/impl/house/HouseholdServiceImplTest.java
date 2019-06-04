package cn.lehome.dispatcher.queue.service.impl.house;

import cn.lehome.base.api.common.service.community.CommunityApiService;
import cn.lehome.base.api.property.service.households.HouseholdsInfoApiService;
import cn.lehome.base.api.user.service.user.UserHouseRelationshipApiService;
import cn.lehome.base.api.user.service.user.UserInfoIndexApiService;
import cn.lehome.dispatcher.queue.service.house.HouseholdService;
import org.junit.After;
import org.junit.Test;
import org.mockito.Mock;
import org.springframework.beans.factory.annotation.Autowired;

import java.lang.reflect.Field;

//@RunWith(SpringJUnit4ClassRunner.class)
//@SpringBootTest(classes = ServiceApplication.class)
public class HouseholdServiceImplTest {


    @Autowired
    private HouseholdService householdService;
    @Autowired
    private UserInfoIndexApiService userInfoIndexApiService;
    @Mock
    private HouseholdsInfoApiService householdsInfoApiService;
    @Mock
    private CommunityApiService communityApiService;
    @Mock
    private UserHouseRelationshipApiService userHouseRelationshipApiService;
    @Autowired
    private UserHouseRelationshipApiService userHouseRelationshipApiService1;
    @Autowired
    private CommunityApiService communityApiService1;

    @Test
    public void syncHouseholdInfo() throws Exception {

//        String openId = "e0edcb88-3a36-4533-8a6c-4062c21562bc";
//
//
//        UserInfoIndex userInfoIndex = userInfoIndexApiService.findByOpenId(openId);
//        List<AuthHouseholdsInfo> freeAuthHouseholdsInfoList = new ArrayList<>();
//        AuthHouseholdsInfo info1 = new AuthHouseholdsInfo();
//        info1.setAreaId(110000L);
//        info1.setHouseId(155L);
//        info1.setHouseHoldTypeId(1L);
//        info1.setAddress("");
//        freeAuthHouseholdsInfoList.add(info1);
//        AuthHouseholdsInfo info2 = new AuthHouseholdsInfo();
//        info2.setAreaId(110000L);
//        info2.setHouseId(105L);
//        info2.setHouseHoldTypeId(1L);
//        info2.setAddress("");
//        freeAuthHouseholdsInfoList.add(info2);
//        when(householdsInfoApiService.relateByPhone(anyString(), anyLong(), anyMap())).thenReturn(freeAuthHouseholdsInfoList);
//        changeMockObject(householdService, householdsInfoApiService);
//
//        when(communityApiService.findByPropertyAreaId(anyLong())).thenReturn(communityApiService1.getExt(1L));
//        changeMockObject(householdService, communityApiService);
//
//        List<UserHouseRelationship> userHouseRelationships = userHouseRelationshipApiService1.findByRemark("13911231323");
//        when(userHouseRelationshipApiService.findByRemark(anyString())).thenReturn(userHouseRelationships);
//        when(userHouseRelationshipApiService.saveUserHouse(any())).thenReturn(userHouseRelationshipApiService1.get(1L));
//        when(userHouseRelationshipApiService.findByUserId(anyLong())).thenReturn(userHouseRelationshipApiService1.findByUserId(189L));
//        when(userHouseRelationshipApiService.update(any())).thenReturn(userHouseRelationshipApiService1.get(1L));
//        changeMockObject(householdService, userHouseRelationshipApiService);
//
//        householdService.syncHouseholdInfo(userInfoIndex);


    }

    @After
    public void after() throws InterruptedException {

//        Thread.sleep(100 * 1000);

    }


    private void changeMockObject(Object origin, Object target) {
        try {
            Field declaredField = origin.getClass().getDeclaredField(target.toString());
            declaredField.setAccessible(true);
            declaredField.set(origin, target);
            declaredField.setAccessible(false);
        } catch (Exception ex) {

        }
    }


}