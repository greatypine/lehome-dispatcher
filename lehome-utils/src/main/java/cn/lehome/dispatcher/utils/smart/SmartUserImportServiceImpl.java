package cn.lehome.dispatcher.utils.smart;

import cn.lehome.dispatcher.utils.smart.bean.Department;
import cn.lehome.dispatcher.utils.smart.bean.RpcRequest;
import cn.lehome.dispatcher.utils.smart.bean.UserInfo;
import cn.lehome.framework.base.api.core.util.CoreHttpUtils;
import cn.lehome.framework.base.api.core.util.ExcelUtils;
import com.alibaba.druid.util.StringUtils;
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by zuoguodong on 2019/9/3
 */
@Service("smartUserImportService")
public class SmartUserImportServiceImpl implements SmartUserImportService{

    @Override
    public void importData(String[] input) {
        String baseUrl = input[1];
        String token = input[2];
        String excelPath = input[3];
        Map<String,String> header = new HashMap<>();
        header.put("Authorization",token);
        header.put("x-user-token",token);
        header.put("Content-Type","application/json");
        List<UserInfo> userInfoList = this.parseExcel(excelPath);
        userInfoList.forEach(userInfo -> {
            RpcRequest request = new RpcRequest();
            request.setMethod("web_Console_StaffMemberCreate_createStaffMember");
            request.getParams().add(userInfo);
            String response = CoreHttpUtils.post(baseUrl,header,JSON.toJSONString(request),String.class).getResponse();
            System.out.println(userInfo.getName() + ":" + response);
        });

    }

    private List<UserInfo> parseExcel(String path){
        List<UserInfo> userInfoList = new ArrayList<>();
        ExcelUtils excelUtils = new ExcelUtils(path);
        try {
            List<List<String>> data = excelUtils.read(0,3,excelUtils.getRowCount(0) - 1);
            for(List<String> l : data){
                String userName = l.get(5);
                if(StringUtils.isEmpty(userName)){
                    System.out.println("用户名为空跳过");
                    continue;
                }
                String userPhone = l.get(8);
                if(StringUtils.isEmpty(userPhone)){
                    System.out.println("电话为空跳过");
                    continue;
                }
                String sex = l.get(6);
                if(StringUtils.isEmpty(sex)){
                    sex = "Male";
                }else{
                    sex = sex.equals("男")?"Male":"Female";
                }
                String dept = l.get(0);
                if(StringUtils.isEmpty(dept)){
                    System.out.println("部门为空跳过");
                    continue;
                }
                UserInfo userInfo = new UserInfo();
                userInfo.setName(userName);
                userInfo.setPhone(userPhone);
                userInfo.setSex(sex);
                userInfo.setPosition(StringUtils.isEmpty(l.get(7))?"":l.get(7));
                userInfo.setNumber("");
                Department department = new Department();
                department.setId(Long.valueOf(dept));
                userInfo.getDepartments().add(department);
                userInfoList.add(userInfo);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return userInfoList;
    }


}
