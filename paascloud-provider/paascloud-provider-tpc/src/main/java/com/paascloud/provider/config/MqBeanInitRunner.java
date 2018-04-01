package com.paascloud.provider.config;

import com.alibaba.fastjson.JSON;
import com.paascloud.base.constant.GlobalConstant;
import com.paascloud.config.properties.PaascloudProperties;
import com.paascloud.core.registry.RegistryCenterFactory;
import com.paascloud.core.registry.base.CoordinatorRegistryCenter;
import com.paascloud.core.registry.base.ReliableMessageRegisterDto;
import com.paascloud.provider.listener.MqConsumerChangeListener;
import com.paascloud.provider.listener.MqProducerChangeListener;
import com.paascloud.provider.service.MqProducerBeanFactory;
import com.paascloud.provider.service.TpcMqProducerService;
import lombok.extern.slf4j.Slf4j;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.cache.TreeCache;
import org.springframework.boot.CommandLineRunner;
import org.springframework.core.Ordered;
import org.springframework.core.annotation.Order;
import org.springframework.stereotype.Component;

import javax.annotation.Resource;
import java.util.List;

/**
 * 启动应用时做特殊处理，这些代码会在SpringApplication的run()方法运行完成之前被执行。
 * 通常用于应用启动前的特殊代码执行、特殊数据加载、垃圾数据清理、微服务的服务发现注册、系统启动成功后的通知等。
 * 相当于Spring的ApplicationListener、Servlet的ServletContextListener
 *
 * @author paascloud.net @gmail.com
 */
@Component
@Order(Ordered.HIGHEST_PRECEDENCE)
@Slf4j
public class MqBeanInitRunner implements CommandLineRunner {
    @Resource
    private PaascloudProperties paascloudProperties;
    @Resource
    private MqProducerChangeListener producerChangeListener;
    @Resource
    private MqConsumerChangeListener consumerChangeListener;
    @Resource
    private TpcMqProducerService tpcMqProducerService;

    /**
     * Run.
     *
     * @param args the args
     */
    @Override
    public void run(String... args) throws Exception {
        //获取zk注册中心
        CoordinatorRegistryCenter zkRegistryCenter =
                RegistryCenterFactory.createCoordinatorRegistryCenter(paascloudProperties.getZk());
        //获取producer子节点 key
        List<String> childrenKeys = zkRegistryCenter.getChildrenKeys(GlobalConstant.ZK_REGISTRY_PRODUCER_ROOT_PATH);
        //启动mq监听
        this.initMqListener(zkRegistryCenter);
        //设置子节点
        for (final String child : childrenKeys) {
            String childFullKey = GlobalConstant.ZK_REGISTRY_PRODUCER_ROOT_PATH
                    + GlobalConstant.Symbol.SLASH
                    + child;
            int count = zkRegistryCenter.getNumChildren(childFullKey);
            if (count == 0) {
                continue;
            }
            String producerString = zkRegistryCenter.getDirectly(childFullKey);
            ReliableMessageRegisterDto producerDto = JSON.parseObject(producerString, ReliableMessageRegisterDto.class);

            MqProducerBeanFactory.buildProducerBean(producerDto);
            try {
                tpcMqProducerService.updateOnLineStatusByPid(producerDto.getProducerGroup());
            } catch (Exception e) {
                log.error("更新生产者状态为离线出现异常, ex={}", e.getMessage(), e);
            }
        }
    }

    private void initMqListener(CoordinatorRegistryCenter coordinatorRegistryCenter) throws Exception {
        CuratorFramework cf = (CuratorFramework) coordinatorRegistryCenter.getRawClient();
        initProducerListener(cf);
        initConsumerListener(cf);
    }


    private void initProducerListener(CuratorFramework cf) throws Exception {
        TreeCache treeCache = new TreeCache(cf, GlobalConstant.ZK_REGISTRY_PRODUCER_ROOT_PATH);
        treeCache.getListenable().addListener(producerChangeListener);
        treeCache.start();
    }

    private void initConsumerListener(CuratorFramework cf) throws Exception {
        TreeCache treeCache = new TreeCache(cf, GlobalConstant.ZK_REGISTRY_CONSUMER_ROOT_PATH);
        treeCache.getListenable().addListener(consumerChangeListener);
        treeCache.start();
    }

}