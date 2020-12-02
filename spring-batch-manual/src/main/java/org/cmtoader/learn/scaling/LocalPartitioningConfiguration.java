package org.cmtoader.learn.scaling;

import com.google.common.collect.ImmutableMap;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.batch.core.partition.support.Partitioner;
import org.springframework.batch.item.ExecutionContext;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.support.ListItemReader;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.task.SimpleAsyncTaskExecutor;

import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

@Configuration
public class LocalPartitioningConfiguration {
    private final StepBuilderFactory stepBuilderFactory;
    private final JobBuilderFactory jobBuilderFactory;

    @Autowired
    public LocalPartitioningConfiguration(StepBuilderFactory stepBuilderFactory, JobBuilderFactory jobBuilderFactory) {
        this.stepBuilderFactory = stepBuilderFactory;
        this.jobBuilderFactory = jobBuilderFactory;
    }

    @Bean
    public Partitioner localArrayPartitioner() {
        return gridSize -> {
            int maxDataCount = 10_000;
            int partitionSize = maxDataCount / gridSize;

            return IntStream.range(0, gridSize)
                            .mapToObj(partitionIndex -> Arrays.asList(partitionIndex, partitionIndex * partitionSize, Math.min(partitionIndex * partitionSize + partitionSize, maxDataCount)))
                            .collect(Collectors.toMap(tuple -> "partition" + tuple.get(0).toString(),
                                                      tuple ->  new ExecutionContext(ImmutableMap.of("minValue", tuple.get(1), "maxValue", tuple.get(2)))));
        };
    }

    @Bean
    @StepScope
    public ListItemReader<String> localParitioningDemoItemReader(@Value("#{stepExecutionContext['minValue']}") Integer minValue,
                                                                 @Value("#{stepExecutionContext['maxValue']}") Integer maxValue) {

        System.out.println(String.format("Reading from %s to %s on thread %s", minValue, maxValue, Thread.currentThread().getName()));

        List<String> items = IntStream.range(minValue, maxValue)
                .mapToObj(Objects::toString)
                .collect(Collectors.toList());

        return new ListItemReader<>(items);
    }

    @Bean
    public ItemWriter<String> localParitioningDemoItemWriter() {
        return items -> items.forEach(it -> System.out.println("Writing item " + it + " on thread " + Thread.currentThread().getName()));
    }

    @Bean
    public Step localSlaveStep() {
        return stepBuilderFactory.get("local-slave-step")
                .<String, String>chunk(100)
                .reader(localParitioningDemoItemReader(null, null))
                .writer(localParitioningDemoItemWriter())
                .build();
    }

    @Bean
    public Step localMasterStep() {
        return stepBuilderFactory.get("local-master.step")
                .partitioner(localSlaveStep().getName(), localArrayPartitioner())
                .step(localSlaveStep())
                .gridSize(100)
                .taskExecutor(new SimpleAsyncTaskExecutor())
                .build();
    }

    @Bean
    public Job localMasterParitioningJob() {
        return jobBuilderFactory.get("local-paritioning-demo-job-5")
                .start(localMasterStep())
                .build();
    }
}
