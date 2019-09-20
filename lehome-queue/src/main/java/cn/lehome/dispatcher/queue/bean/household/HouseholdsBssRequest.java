package cn.lehome.dispatcher.queue.bean.household;

import cn.lehome.bean.pro.enums.*;

/**
 * Created by hanxd on 2018/8/22.
 */
public class HouseholdsBssRequest {

    private Integer id;

    private String phone;

    private Integer houseId;


    //激活状态
    private EnabledStatus isActivated;

    //申请途径
    private ApplyChannel applyChannel;

    //姓名
    private String name;

    //性别
    private Gender sex;

    //出生日期
    private String birthday;

    //身份
    private Identity identity;

    //儿童
    private Boolean ischild;

    //老人
    private Boolean isold;

    //在住
    private Boolean islivein;

    //国籍
    private String nation;

    //民族
    private String ethnicity;

    //政治面貌
    private PoliticalStatus politicalstatus;

    //身份证号
    private String idcard;

    //户籍所在地
    private String hukou;

    //工作单位
    private String workplace;

    //单位地址
    private String workplaceAddress;

    //单位电话
    private String workplaceTel;

    //电子邮箱
    private String eMail;

    //qq
    private String qq;

    //微信
    private String wechat;

    //地址
    private String houseAddress;

    //房屋
    private String houseName;

    //备用电话
    private String sparePhone;

    //备用地址
    private String spareHouseAddress;


    public Integer getId() {
        return id;
    }

    public void setId(Integer id) {
        this.id = id;
    }

    public String getPhone() {
        return phone;
    }

    public void setPhone(String phone) {
        this.phone = phone;
    }

    public Integer getHouseId() {
        return houseId;
    }

    public void setHouseId(Integer houseId) {
        this.houseId = houseId;
    }

    public EnabledStatus getIsActivated() {
        return isActivated;
    }

    public void setIsActivated(EnabledStatus isActivated) {
        this.isActivated = isActivated;
    }

    public ApplyChannel getApplyChannel() {
        return applyChannel;
    }

    public void setApplyChannel(ApplyChannel applyChannel) {
        this.applyChannel = applyChannel;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public Gender getSex() {
        return sex;
    }

    public void setSex(Gender sex) {
        this.sex = sex;
    }

    public String getBirthday() {
        return birthday;
    }

    public void setBirthday(String birthday) {
        this.birthday = birthday;
    }

    public Identity getIdentity() {
        return identity;
    }

    public void setIdentity(Identity identity) {
        this.identity = identity;
    }

    public Boolean getIschild() {
        return ischild;
    }

    public void setIschild(Boolean ischild) {
        this.ischild = ischild;
    }

    public Boolean getIsold() {
        return isold;
    }

    public void setIsold(Boolean isold) {
        this.isold = isold;
    }

    public Boolean getIslivein() {
        return islivein;
    }

    public void setIslivein(Boolean islivein) {
        this.islivein = islivein;
    }

    public String getNation() {
        return nation;
    }

    public void setNation(String nation) {
        this.nation = nation;
    }

    public String getEthnicity() {
        return ethnicity;
    }

    public void setEthnicity(String ethnicity) {
        this.ethnicity = ethnicity;
    }

    public PoliticalStatus getPoliticalstatus() {
        return politicalstatus;
    }

    public void setPoliticalstatus(PoliticalStatus politicalstatus) {
        this.politicalstatus = politicalstatus;
    }

    public String getIdcard() {
        return idcard;
    }

    public void setIdcard(String idcard) {
        this.idcard = idcard;
    }

    public String getHukou() {
        return hukou;
    }

    public void setHukou(String hukou) {
        this.hukou = hukou;
    }

    public String getWorkplace() {
        return workplace;
    }

    public void setWorkplace(String workplace) {
        this.workplace = workplace;
    }

    public String getWorkplaceAddress() {
        return workplaceAddress;
    }

    public void setWorkplaceAddress(String workplaceAddress) {
        this.workplaceAddress = workplaceAddress;
    }

    public String getWorkplaceTel() {
        return workplaceTel;
    }

    public void setWorkplaceTel(String workplaceTel) {
        this.workplaceTel = workplaceTel;
    }

    public String geteMail() {
        return eMail;
    }

    public void seteMail(String eMail) {
        this.eMail = eMail;
    }

    public String getQq() {
        return qq;
    }

    public void setQq(String qq) {
        this.qq = qq;
    }

    public String getWechat() {
        return wechat;
    }

    public void setWechat(String wechat) {
        this.wechat = wechat;
    }

    public String getHouseAddress() {
        return houseAddress;
    }

    public void setHouseAddress(String houseAddress) {
        this.houseAddress = houseAddress;
    }

    public String getHouseName() {
        return houseName;
    }

    public void setHouseName(String houseName) {
        this.houseName = houseName;
    }

    public String getSparePhone() {
        return sparePhone;
    }

    public void setSparePhone(String sparePhone) {
        this.sparePhone = sparePhone;
    }

    public String getSpareHouseAddress() {
        return spareHouseAddress;
    }

    public void setSpareHouseAddress(String spareHouseAddress) {
        this.spareHouseAddress = spareHouseAddress;
    }
}
