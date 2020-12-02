package org.cmtoader.learn;

import io.vavr.control.Try;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.JobRegistry;
import org.springframework.batch.core.configuration.annotation.DefaultBatchConfigurer;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.batch.core.configuration.support.JobRegistryBeanPostProcessor;
import org.springframework.batch.core.converter.DefaultJobParametersConverter;
import org.springframework.batch.core.explore.JobExplorer;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.batch.core.launch.JobOperator;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.batch.core.launch.support.SimpleJobLauncher;
import org.springframework.batch.core.launch.support.SimpleJobOperator;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.step.tasklet.Tasklet;
import org.springframework.batch.repeat.RepeatStatus;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.task.SimpleAsyncTaskExecutor;

@Configuration
public class JobConfiguration extends DefaultBatchConfigurer {

    private final JobBuilderFactory jobBuilderFactory;
    private final StepBuilderFactory stepBuilderFactory;
    private final JobLauncher jobLauncher;
    private final JobRepository jobRepository;
    private final JobExplorer jobExplorer;
    private final JobRegistry jobRegistry;
    private final ApplicationContext applicationContext;

    @Autowired
    public JobConfiguration(JobBuilderFactory jobBuilderFactory,
                            StepBuilderFactory stepBuilderFactory,
                            JobLauncher jobLauncher,
                            JobRepository jobRepository,
                            JobExplorer jobExplorer,
                            JobRegistry jobRegistry,
                            ApplicationContext applicationContext) {

        this.jobBuilderFactory = jobBuilderFactory;
        this.stepBuilderFactory = stepBuilderFactory;
        this.jobLauncher = jobLauncher;
        this.jobRepository = jobRepository;
        this.jobExplorer = jobExplorer;
        this.jobRegistry = jobRegistry;
        this.applicationContext = applicationContext;
    }

    @Bean
    public JobOperator jobOperator() throws Exception {
        SimpleJobOperator simpleJobOperator = new SimpleJobOperator();

        simpleJobOperator.setJobLauncher(jobLauncher);
        simpleJobOperator.setJobParametersConverter(new DefaultJobParametersConverter());
        simpleJobOperator.setJobRepository(jobRepository);
        simpleJobOperator.setJobExplorer(jobExplorer);
        simpleJobOperator.setJobRegistry(jobRegistry);

        simpleJobOperator.afterPropertiesSet();

        return simpleJobOperator;
    }

    @Bean
    public JobRegistryBeanPostProcessor jobRegistryBeanPostProcessor() throws Exception {
        JobRegistryBeanPostProcessor registrar = new JobRegistryBeanPostProcessor();
        registrar.setJobRegistry(this.jobRegistry);
        registrar.setBeanFactory(this.applicationContext.getAutowireCapableBeanFactory());
        registrar.afterPropertiesSet();
        return registrar;
    }

    @Bean
    @StepScope
    public Tasklet tasklet(@Value("#{jobParameters['name']}") String name) {
        return (stepContribution, chunkContext) -> {
            System.out.println("Launched tasklet with name " + name);
            return RepeatStatus.FINISHED;
        };
    }

    @Bean
    @StepScope
    public Tasklet taskletSleepy(@Value("#{jobParameters['name']}") String name) {
        return (stepContribution, chunkContext) -> {
            System.out.println("Launched tasklet with name " + name);
            System.out.println("Sleeping a little.");
            Thread.sleep(1000);
            return RepeatStatus.CONTINUABLE;
        };
    }

    @Bean
    public Step step1() {
        return stepBuilderFactory.get("step-1")
                .tasklet(tasklet(null))
                .build();
    }

    @Bean
    public Job job() {
        return jobBuilderFactory.get("job")
                .incrementer(new RunIdIncrementer()) // the job needs to be called with job.startNextInstance()
                .start(step1())
                .build();
    }

    @Bean
    public Step step2() {
        return stepBuilderFactory.get("step-2")
                .tasklet(tasklet(null))
                .build();
    }

    @Bean
    public Job sleepyJob() {
        return jobBuilderFactory.get("sleepyJob")
                .start(step2())
                .build();
    }

    @Override
    protected JobLauncher createJobLauncher() {
        Try<SimpleJobLauncher> simpleJobLauncherTry = Try.of(() -> {
            SimpleJobLauncher jobLauncher = new SimpleJobLauncher();
            jobLauncher.setJobRepository(jobRepository);
            jobLauncher.setTaskExecutor(new SimpleAsyncTaskExecutor());
            jobLauncher.afterPropertiesSet();
            return jobLauncher;
        });

        return simpleJobLauncherTry.getOrElseThrow(ex -> new RuntimeException(ex));
    }
}
