package org.cmtoader.learn.reader;

import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.item.ItemReader;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

@Configuration
public class SimpleItemReaderConfiguration {

    private final JobBuilderFactory jobBuilderFactory;
    private final StepBuilderFactory stepBuilderFactory;

    @Autowired
    public SimpleItemReaderConfiguration(JobBuilderFactory jobBuilderFactory, StepBuilderFactory stepBuilderFactory) {
        this.jobBuilderFactory = jobBuilderFactory;
        this.stepBuilderFactory = stepBuilderFactory;
    }
 
    @Bean
    public ItemReader<String> simpleItemReader() {
        List<String> strings = Arrays.asList("one", "two", "three", "four", "five", "six");
        return new SimpleItemReader(strings.iterator());
    }

    @Bean
    public Step simpleReaderStep() {
        return stepBuilderFactory.get("simple-reader-step")
                .<String, String>chunk(2)
                .reader(simpleItemReader())
                .writer(items -> System.out.println(items.stream().collect(Collectors.joining(","))))
                .build();
    }

    @Bean
    public Job simpleReaderJob() {
        return jobBuilderFactory.get("simple-reader-job-2")
                .start(simpleReaderStep())
                .build();
    }
}
