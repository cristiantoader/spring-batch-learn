package org.cmtoader.learn.errors;

import io.micrometer.core.instrument.util.StringUtils;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.support.ListItemReader;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

@Configuration
public class SkipErrorJobConfiguration {

    private final StepBuilderFactory stepBuilderFactory;
    private final JobBuilderFactory jobBuilderFactory;

    @Autowired
    public SkipErrorJobConfiguration(StepBuilderFactory stepBuilderFactory, JobBuilderFactory jobBuilderFactory) {
        this.stepBuilderFactory = stepBuilderFactory;
        this.jobBuilderFactory = jobBuilderFactory;
    }

    @Bean
    @StepScope
    public ListItemReader<String> skipItemReader() {
        List<String> items = IntStream.range(0, 100)
                .mapToObj(Objects::toString)
                .collect(Collectors.toList());

        return new ListItemReader<>(items);
    }

    @Bean
    @StepScope
    public ItemProcessor<String, String> skipItemProcessor() {
        return item -> {
            System.out.println("Processing item " + item);
            return item;
        };
    }

    @Bean
    @StepScope
    SkipItemWriter skipItemWriter(@Value("#{jobParameters['skip']}") String skip) {
        boolean shouldSkip = StringUtils.isNotBlank(skip) && skip.equalsIgnoreCase("writer");
        return new SkipItemWriter(shouldSkip);
    }

    @Bean
    public Step skipItemStep() {
        return stepBuilderFactory.get("skip-item-step")
                .<String, String>chunk(10)
                .reader(skipItemReader())
                .processor(skipItemProcessor())
                .writer(skipItemWriter(null))
                .faultTolerant()
                .skip(CustomRetryableException.class)
                .skipLimit(10)
                .listener(new CustomSkipListener())
                .build();
    }

    @Bean
    public Job skipItemJob() {
        return jobBuilderFactory.get("skip-item-job-3")
                .start(skipItemStep())
                .build();
    }
}
