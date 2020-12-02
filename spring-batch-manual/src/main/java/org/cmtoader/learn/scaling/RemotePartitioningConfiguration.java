package org.cmtoader.learn.scaling;

import com.google.common.collect.ImmutableMap;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.batch.core.explore.JobExplorer;
import org.springframework.batch.core.partition.PartitionHandler;
import org.springframework.batch.core.partition.support.Partitioner;
import org.springframework.batch.integration.partition.BeanFactoryStepLocator;
import org.springframework.batch.integration.partition.MessageChannelPartitionHandler;
import org.springframework.batch.integration.partition.StepExecutionRequestHandler;
import org.springframework.batch.item.ExecutionContext;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.support.ListItemReader;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Profile;
import org.springframework.core.task.SimpleAsyncTaskExecutor;
import org.springframework.integration.annotation.ServiceActivator;
import org.springframework.integration.core.MessagingTemplate;
import org.springframework.integration.scheduling.PollerMetadata;
import org.springframework.scheduling.support.PeriodicTrigger;

import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

@Configuration
public class RemotePartitioningConfiguration {
    private static final int GRID_SIZE = 100;

    private final StepBuilderFactory stepBuilderFactory;
    private final JobBuilderFactory jobBuilderFactory;
    private final JobExplorer jobExplorer;
    private final ApplicationContext applicationContext;

    @Autowired
    public RemotePartitioningConfiguration(StepBuilderFactory stepBuilderFactory,
                                           JobBuilderFactory jobBuilderFactory,
                                           JobExplorer jobExplorer,
                                           ApplicationContext applicationContext) {

        this.stepBuilderFactory = stepBuilderFactory;
        this.jobBuilderFactory = jobBuilderFactory;
        this.jobExplorer = jobExplorer;
        this.applicationContext = applicationContext;
    }

    @Bean
    public PartitionHandler partitionHandler(MessagingTemplate messagingTemplate) throws Exception {
        MessageChannelPartitionHandler partitionHandler = new MessageChannelPartitionHandler();

        partitionHandler.setStepName("remoteSlaveStep"); // bean id in the slave jvm
        partitionHandler.setGridSize(GRID_SIZE);
        partitionHandler.setMessagingOperations(messagingTemplate);
        partitionHandler.setPollInterval(5000l);  // how to know slave is done
        partitionHandler.setJobExplorer(jobExplorer);

        partitionHandler.afterPropertiesSet();

        return partitionHandler;
    }

    @Bean
    @Profile("slave")
    @ServiceActivator(inputChannel = "inboundRequests", outputChannel = "outboundStaging")
    public StepExecutionRequestHandler stepExecutionRequestHandler() {
        StepExecutionRequestHandler stepExecutionRequestHandler = new StepExecutionRequestHandler();

        BeanFactoryStepLocator stepLocator = new BeanFactoryStepLocator();
        stepLocator.setBeanFactory(this.applicationContext);

        stepExecutionRequestHandler.setStepLocator(stepLocator);
        stepExecutionRequestHandler.setJobExplorer(jobExplorer);

        return stepExecutionRequestHandler;
    }

    @Bean(name = PollerMetadata.DEFAULT_POLLER)
    public PollerMetadata defaultPoller() {
        PollerMetadata pollerMetadata = new PollerMetadata();
        pollerMetadata.setTrigger(new PeriodicTrigger(10));
        return pollerMetadata;
    }

    @Bean
    public Partitioner remoteArrayPartitioner() {
        return gridSize -> {
            int maxDataCount = 10_000;
            int partitionSize = maxDataCount / gridSize;

            return IntStream.range(0, gridSize)
                    .mapToObj(partitionIndex -> Arrays.asList(partitionIndex, partitionIndex * partitionSize, Math.min(partitionIndex * partitionSize + partitionSize, maxDataCount)))
                    .collect(Collectors.toMap(tuple -> "partition" + tuple.get(0).toString(),
                            tuple -> new ExecutionContext(ImmutableMap.of("minValue", tuple.get(1), "maxValue", tuple.get(2)))));
        };
    }

    @Bean
    @StepScope
    public ListItemReader<String> remoteParitioningDemoItemReader(@Value("#{stepExecutionContext['minValue']}") Integer minValue,
                                                                  @Value("#{stepExecutionContext['maxValue']}") Integer maxValue) {

        System.out.println(String.format("Reading from %s to %s on thread %s", minValue, maxValue, Thread.currentThread().getName()));

        List<String> items = IntStream.range(minValue, maxValue)
                .mapToObj(Objects::toString)
                .collect(Collectors.toList());

        return new ListItemReader<>(items);
    }

    @Bean
    public ItemWriter<String> remoteParitioningDemoItemWriter() {
        return items -> items.forEach(it -> System.out.println("Writing item " + it + " on thread " + Thread.currentThread().getName()));
    }

    @Bean
    public Step remoteSlaveStep() {
        return stepBuilderFactory.get("remote-slave-step")
                .<String, String>chunk(100)
                .reader(remoteParitioningDemoItemReader(null, null))
                .writer(remoteParitioningDemoItemWriter())
                .build();
    }

    @Bean
    public Step remoteMasterStep() throws Exception {
        return stepBuilderFactory.get("remote-master-step")
                .partitioner(remoteSlaveStep().getName(), remoteArrayPartitioner())
                .partitionHandler(partitionHandler(null))
                .step(remoteSlaveStep())
                .gridSize(GRID_SIZE)
                .build();
    }

    @Bean
    public Job remoteMasterParitioningJob() throws Exception {
        return jobBuilderFactory.get("remote-paritioning-demo-job-5")
                .start(remoteMasterStep())
                .build();
    }
}
