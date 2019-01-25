package cn.lehome.dispatcher.utils.es.client;

import cn.lehome.dispatcher.utils.es.config.EsConfig;
import cn.lehome.dispatcher.utils.es.exception.ClientInitException;
import com.google.common.collect.Lists;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.common.transport.TransportAddress;

import java.net.InetAddress;
import java.util.ArrayList;

/**
 * Created by zhanghuan on 2018/7/19.
 */
public class EsClient {
    private static EsClient esClient = new EsClient();
    private Client client;



    public static EsClient getEsClient() {
        return esClient;
    }

    private EsClient() {
        try {
            String clusterNodes= EsConfig.CONFIG_MAP.get("clusterNodes");
            String clusterName = EsConfig.CONFIG_MAP.get("clusterName");
            Settings settings = Settings.settingsBuilder().put("cluster.name", clusterName).build();
            String[] urls = clusterNodes.split(",");
            ArrayList transportAddresses = Lists.newArrayList();
            String[] var5 = urls;
            int var6 = urls.length;

            for(int var7 = 0; var7 < var6; ++var7) {
                String url = var5[var7];
                String[] strs = url.split(":");
                transportAddresses.add(new InetSocketTransportAddress(InetAddress.getByName(strs[0]), Integer.valueOf(strs[1]).intValue()));
            }

            this.client = TransportClient.builder().settings(settings).build().addTransportAddresses((TransportAddress[])transportAddresses.toArray(new InetSocketTransportAddress[transportAddresses.size()]));
        } catch (Exception var10) {
            throw new ClientInitException("init es client error", var10);
        }
    }

    public Client getClient() {
        return this.client;
    }
}

