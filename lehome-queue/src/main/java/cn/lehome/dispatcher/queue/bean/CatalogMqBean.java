package cn.lehome.dispatcher.queue.bean;

import java.io.Serializable;

/**
 * 商品分类修改MQ请求体
 */
public class CatalogMqBean implements Serializable {

    private static final long serialVersionUID = 8989831309206337700L;

    private String catalogName;

    private Long catalogId;

    private Long parentCatalogId;

    public CatalogMqBean() {}

    public CatalogMqBean(String catalogName, Long id, Long parentCatalogId) {
        this.catalogId = id;
        this.catalogName = catalogName;
        this.parentCatalogId = parentCatalogId;
    }

    public Long getParentCatalogId() {
        return parentCatalogId;
    }

    public void setParentCatalogId(Long parentCatalogId) {
        this.parentCatalogId = parentCatalogId;
    }

    public String getCatalogName() {
        return catalogName;
    }

    public void setCatalogName(String catalogName) {
        this.catalogName = catalogName;
    }

    public Long getCatalogId() {
        return catalogId;
    }

    public void setCatalogId(Long catalogId) {
        this.catalogId = catalogId;
    }
}