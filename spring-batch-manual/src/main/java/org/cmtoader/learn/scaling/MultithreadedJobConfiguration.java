package org.cmtoader.learn.scaling;

import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.batch.integration.async.AsyncItemProcessor;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.support.ListItemReader;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.task.SimpleAsyncTaskExecutor;

import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

@Configuration
public class MultithreadedJobConfiguration {

    private final StepBuilderFactory stepBuilderFactory;
    private final JobBuilderFactory jobBuilderFactory;

    @Autowired
    public MultithreadedJobConfiguration(StepBuilderFactory stepBuilderFactory, JobBuilderFactory jobBuilderFactory) {
        this.stepBuilderFactory = stepBuilderFactory;
        this.jobBuilderFactory = jobBuilderFactory;
    }

    @Bean
    @StepScope
    public ListItemReader<String> multithreadedDemoItemReader() {
        List<String> items = IntStream.range(0, 100)
                .mapToObj(Objects::toString)
                .collect(Collectors.toList());

        return new ListItemReader<>(items);
    }

    @Bean
    @StepScope
    public ItemWriter<String> multithreadedDemoItemWriter() {
        return items -> System.out.println(" Writing items " + items + " on thread "+ Thread.currentThread().getName());
    }

    @Bean
    public Step multithreadedDemoStep() {
        return stepBuilderFactory.get("multithreaded-demo-step")
                .<String, String>chunk(10)
                .reader(multithreadedDemoItemReader())
                .writer(multithreadedDemoItemWriter())
                .taskExecutor(new SimpleAsyncTaskExecutor())
                .build();
    }

    @Bean
    public Job multithreadedDemoJob() {
        return jobBuilderFactory.get("multithreaded-demo-job")
                .start(multithreadedDemoStep())
                .build();
    }


}
