package org.cmtoader.learn;

import io.vavr.control.Try;
import org.springframework.batch.core.*;
import org.springframework.batch.core.launch.JobInstanceAlreadyExistsException;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.batch.core.launch.JobOperator;
import org.springframework.batch.core.launch.NoSuchJobException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;

import static io.vavr.API.$;
import static io.vavr.API.Case;
import static io.vavr.Predicates.instanceOf;

@RestController
@RequestMapping("/job/operator")
public class JobOperatorController {

    private final JobOperator jobOperator;

    @Autowired
    public JobOperatorController(JobOperator jobOperator) {
        this.jobOperator = jobOperator;
    }

    @PostMapping
    public Long launchJob(@RequestParam("name") String name) {
        // looks in job registry for job named job and starts with name parameter
        return Try.of(() -> this.jobOperator.start("job", String.format("name=%s", name)))
                  .getOrElseThrow((ex) -> new IllegalStateException(ex));
    }

    @DeleteMapping("/{id}")
    public void deleteJob(@PathVariable("id") Long id) {
        // will change job status to stopping
        Try.of(() -> this.jobOperator.stop(id))
           .getOrElseThrow(ex -> new IllegalStateException(ex));
    }

}
