package cn.lehome.dispatcher.queue.bean;

import cn.lehome.bean.business.activity.enums.task.AssetType;
import cn.lehome.bean.business.activity.enums.task.TaskDrewStatus;
import cn.lehome.bean.business.activity.enums.task.UserTaskStatus;
import cn.lehome.framework.bean.core.enums.Operation;
import cn.lehome.framework.bean.core.enums.OperationType;
import cn.lehome.framework.bean.core.enums.UserType;
import cn.lehome.framework.bean.core.enums.YesNoStatus;

import java.io.Serializable;
import java.util.Date;

/**
 * Created by zuoguodong on 2018/5/16
 */
public class UserOperationRecord implements Serializable {

    //用户ID或系统管理员ID
    private String objectId;

    private UserType userType;

    //操作类型
    private OperationType operationType;

    //操作
    private Operation operation;

    //操作数量
    private Long operationNum;

    //资产类型
    private AssetType assetType;

    //获得的奖金数
    private Long amount;

    //获取的金豆数量
    private Long beanAmount;

    //完成个数
    private Long completeCount;

    //领取奖金数量
    private Long drawAmount;

    //领取金豆数量
    private Long drawBeanAmount;

    //已经领取次数
    private Long drawCount;

    private String businessId;

    private String originId;

    private Long communityId;

    private UserTaskStatus userTaskStatus;

    private Date completeTime;

    private String phone;

    private TaskDrewStatus drewStatus;

    private String originUserId;

    /**
     * 是否是虚拟机注册登录的用户
     */
    private YesNoStatus isSimulator;

    public YesNoStatus getIsSimulator() {
        return isSimulator;
    }

    public void setIsSimulator(YesNoStatus isSimulator) {
        this.isSimulator = isSimulator;
    }

    public String getOriginUserId() {
        return originUserId;
    }

    public void setOriginUserId(String originUserId) {
        this.originUserId = originUserId;
    }

    public String getObjectId() {
        return objectId;
    }

    public void setObjectId(String objectId) {
        this.objectId = objectId;
    }

    public UserType getUserType() {
        return userType;
    }

    public void setUserType(UserType userType) {
        this.userType = userType;
    }

    public OperationType getOperationType() {
        return operationType;
    }

    public void setOperationType(OperationType operationType) {
        this.operationType = operationType;
    }

    public Operation getOperation() {
        return operation;
    }

    public void setOperation(Operation operation) {
        this.operation = operation;
    }

    public Long getOperationNum() {
        return operationNum;
    }

    public void setOperationNum(Long operationNum) {
        this.operationNum = operationNum;
    }

    public AssetType getAssetType() {
        return assetType;
    }

    public void setAssetType(AssetType assetType) {
        this.assetType = assetType;
    }

    public Long getAmount() {
        return amount;
    }

    public void setAmount(Long amount) {
        this.amount = amount;
    }

    public Long getBeanAmount() {
        return beanAmount;
    }

    public void setBeanAmount(Long beanAmount) {
        this.beanAmount = beanAmount;
    }

    public Long getCompleteCount() {
        return completeCount;
    }

    public void setCompleteCount(Long completeCount) {
        this.completeCount = completeCount;
    }

    public Long getDrawAmount() {
        return drawAmount;
    }

    public void setDrawAmount(Long drawAmount) {
        this.drawAmount = drawAmount;
    }

    public Long getDrawBeanAmount() {
        return drawBeanAmount;
    }

    public void setDrawBeanAmount(Long drawBeanAmount) {
        this.drawBeanAmount = drawBeanAmount;
    }

    public Long getDrawCount() {
        return drawCount;
    }

    public void setDrawCount(Long drawCount) {
        this.drawCount = drawCount;
    }

    public String getBusinessId() {
        return businessId;
    }

    public void setBusinessId(String businessId) {
        this.businessId = businessId;
    }

    public Long getCommunityId() {
        return communityId;
    }

    public void setCommunityId(Long communityId) {
        this.communityId = communityId;
    }

    public UserTaskStatus getUserTaskStatus() {
        return userTaskStatus;
    }

    public void setUserTaskStatus(UserTaskStatus userTaskStatus) {
        this.userTaskStatus = userTaskStatus;
    }

    public Date getCompleteTime() {
        return completeTime;
    }

    public void setCompleteTime(Date completeTime) {
        this.completeTime = completeTime;
    }

    public String getPhone() {
        return phone;
    }

    public void setPhone(String phone) {
        this.phone = phone;
    }

    public TaskDrewStatus getDrewStatus() {
        return drewStatus;
    }

    public void setDrewStatus(TaskDrewStatus drewStatus) {
        this.drewStatus = drewStatus;
    }

    public String getOriginId() {
        return originId;
    }

    public void setOriginId(String originId) {
        this.originId = originId;
    }
}
