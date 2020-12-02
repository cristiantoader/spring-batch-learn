package org.cmtoader.learn.scaling;

import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.batch.integration.async.AsyncItemProcessor;
import org.springframework.batch.integration.async.AsyncItemWriter;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.support.ListItemReader;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.task.SimpleAsyncTaskExecutor;

import java.util.List;
import java.util.Objects;
import java.util.function.Consumer;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

@Configuration
public class AsyncJobConfiguration {

    private final StepBuilderFactory stepBuilderFactory;
    private final JobBuilderFactory jobBuilderFactory;

    @Autowired
    public AsyncJobConfiguration(StepBuilderFactory stepBuilderFactory, JobBuilderFactory jobBuilderFactory) {
        this.stepBuilderFactory = stepBuilderFactory;
        this.jobBuilderFactory = jobBuilderFactory;
    }

    @Bean
    @StepScope
    public ListItemReader<String> asyncDemoItemReader() {
        List<String> items = IntStream.range(0, 100)
                .mapToObj(Objects::toString)
                .collect(Collectors.toList());

        return new ListItemReader<>(items);
    }

    @Bean
    public ItemProcessor<String, String> asyncInnerItemProcessor() {
        return item -> {
            System.out.println("Processing item " + item + " on thread " + Thread.currentThread().getName());
            Thread.sleep(10);
            return String.valueOf(Integer.valueOf(item) * -1);
        };
    }

    @Bean
    public AsyncItemProcessor asyncItemProcessor() throws Exception {
        AsyncItemProcessor<String, String> asyncItemProcessor = new AsyncItemProcessor<>();
        asyncItemProcessor.setDelegate(asyncInnerItemProcessor());
        asyncItemProcessor.setTaskExecutor(new SimpleAsyncTaskExecutor());
        asyncItemProcessor.afterPropertiesSet();
        return asyncItemProcessor;
    }

    @Bean
    public ItemWriter<String> asyncInnerItemWriter() {
        return items -> items.forEach(it -> System.out.println("Writing item " + it + " on thread " + Thread.currentThread().getName()));
    }

    @Bean
    public AsyncItemWriter<String> asyncItemWriter() throws Exception {
        AsyncItemWriter<String> asyncItemWriter = new AsyncItemWriter<>();
        asyncItemWriter.setDelegate(asyncInnerItemWriter());
        asyncItemWriter.afterPropertiesSet();
        return asyncItemWriter;
    }

    @Bean
    public Step asyncStep() {
        return stepBuilderFactory.get("async-step-1")
                .<String, String>chunk(10)
                .reader(asyncDemoItemReader())
                .processor(wrapSupplier(this::asyncItemProcessor).get())
                .writer(wrapSupplier(this::asyncItemWriter).get())
                .build();
    }

    @Bean
    public Job asyncJob() {
        return jobBuilderFactory.get("async-job-1")
                .start(asyncStep())
                .build();
    }

    private static <T> Supplier<T> wrapSupplier(ExceptionalSupplier<T> supplier) {
        return () -> {
            try {
                return supplier.supply();
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        };
    }

    private interface ExceptionalSupplier<T> {
        T supply() throws Exception;
    }
}
