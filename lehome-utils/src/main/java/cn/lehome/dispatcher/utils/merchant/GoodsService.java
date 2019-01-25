package cn.lehome.dispatcher.utils.merchant;

/**
 * Created by zhuhai on 2018/8/21
 */
public interface GoodsService {

    void initMerchantIndex(String input[]);

    void refreshGoodsNum(String[] input);
}
