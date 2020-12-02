package org.cmtoader.learn;

import io.vavr.control.Try;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.JobParameters;
import org.springframework.batch.core.JobParametersBuilder;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/job/launcher")
public class JobLauncherController {

    private final JobLauncher jobLauncher;
    private final Job job;

    @Autowired
    public JobLauncherController(JobLauncher jobLauncher, Job job) {
        this.jobLauncher = jobLauncher;
        this.job = job;
    }

    @PostMapping
    public Long launchJob(@RequestParam("name") String name) {
        JobParameters jobParameters = new JobParametersBuilder()
                .addString("name", name)
                .toJobParameters();

        return Try.of(() -> this.jobLauncher.run(job, jobParameters))
                  .map(JobExecution::getJobId)
                  .getOrElseThrow(() -> new IllegalStateException("Could not execute job"));
    }

}
