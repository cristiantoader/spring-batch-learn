package org.cmtoader.learn.listener;

import org.cmtoader.learn.listener.ChunkListener;
import org.cmtoader.learn.listener.JobListener;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.support.ListItemReader;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.Arrays;

@Configuration
public class ListenerJobConfiguration {

    private final JobBuilderFactory jobBuilderFactory;
    private final StepBuilderFactory stepBuilderFactory;

    @Autowired
    public ListenerJobConfiguration(JobBuilderFactory jobBuilderFactory, StepBuilderFactory stepBuilderFactory) {
        this.jobBuilderFactory = jobBuilderFactory;
        this.stepBuilderFactory = stepBuilderFactory;
    }

    @Bean
    public ItemReader<String> listenerItemReader() {
        return new ListItemReader<>(Arrays.asList("one", "two", "three", "four"));
    }

    @Bean
    public ItemWriter<String> listenerItemWriter() {
        return items -> items.stream().map(it -> "Writing item " + it).forEach(System.out::println);
    }

    @Bean
    public Step listenerStep() {
        return stepBuilderFactory.get("listener-step")
                .<String, String>chunk(2) // generic types for IO
                .faultTolerant()
                .listener(new ChunkListener())
                .reader(listenerItemReader())
                .writer(listenerItemWriter())
                .build();
    }

    @Bean
    public Job listenerJob() {
        return jobBuilderFactory.get("listener-job-4")
                .start(listenerStep())
                .listener(new JobListener())
                .build();
    }
}
