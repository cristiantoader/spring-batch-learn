package org.cmtoader.learn.errors;

import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.support.ListItemReader;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.util.StringUtils;

import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

@Configuration
public class RetryErrorJobConfiguration {

    private final StepBuilderFactory stepBuilderFactory;
    private final JobBuilderFactory jobBuilderFactory;

    @Autowired
    public RetryErrorJobConfiguration(StepBuilderFactory stepBuilderFactory, JobBuilderFactory jobBuilderFactory) {
        this.stepBuilderFactory = stepBuilderFactory;
        this.jobBuilderFactory = jobBuilderFactory;
    }

    @Bean
    @StepScope
    public ListItemReader<String> retryItemReader() {
        List<String> items = IntStream.range(0, 100)
                                      .mapToObj(Objects::toString)
                                      .collect(Collectors.toList());

        return new ListItemReader<>(items);
    }

    @Bean
    @StepScope
    public RetryItemProcessor retryItemProcessor(@Value("#{jobParameters['retry']}") String retry) {
        boolean failAndRetry = StringUtils.hasText(retry) && retry.equalsIgnoreCase("processor");
        return new RetryItemProcessor(failAndRetry);
    }

    @Bean
    @StepScope
    public ItemWriter<String> retryItemWriter() {
        return batch -> batch.forEach(System.out::println);
    }

    @Bean
    public Step retryDemoStep1() {
        return stepBuilderFactory.get("retry-demo-step1")
                .<String, String>chunk(10)
                .reader(retryItemReader())
                .processor(retryItemProcessor(null))
                .writer(retryItemWriter())
                .faultTolerant()
                .retry(CustomRetryableException.class)
                .retryLimit(15)
                .build();
    }
    @Bean
    public Job retryDemoJob() {
        return jobBuilderFactory.get("retry-demo-job")
                .start(retryDemoStep1())
                .build();
    }
}
