package com.yinian.alysis.config.Schedule;

import org.springframework.context.ApplicationListener;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.event.ContextRefreshedEvent;
import org.springframework.scheduling.quartz.SchedulerFactoryBean;

/** 
 * @ClassName: SchedulerListener 
 * @Description: TODO(这里用一句话描述这个类的作用) 
 * @author 刘猛
 * @date 2018�?6�?10�? 上午10:24:59
 */
@Configuration
public class SchedulerListener implements ApplicationListener<ContextRefreshedEvent> {
   /* @Autowired
    public SyncScheduler myScheduler;*/

    @Override
    public void onApplicationEvent(ContextRefreshedEvent event) {
       /* try {
            myScheduler.scheduleJobs();
        } catch (SchedulerException e) {
            e.printStackTrace();
        }*/

    }

    @Bean
    public SchedulerFactoryBean schedulerFactoryBean(){
        SchedulerFactoryBean schedulerFactoryBean = new SchedulerFactoryBean();
        return schedulerFactoryBean;
    }

}
