package com.di.streamnova;

import com.di.streamnova.runner.DataflowRunnerService;
import com.di.streamnova.util.NullFilteringPrintStream;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.autoconfigure.jdbc.DataSourceAutoConfiguration;
import org.springframework.boot.autoconfigure.jdbc.DataSourceTransactionManagerAutoConfiguration;
import org.springframework.boot.context.properties.ConfigurationPropertiesScan;
import org.springframework.boot.context.properties.EnableConfigurationProperties;

import com.di.streamnova.agent.execution_planner.MachineLadderProperties;
import com.di.streamnova.config.YamlPipelineProperties;

@SpringBootApplication(exclude = {
		DataSourceAutoConfiguration.class,
		DataSourceTransactionManagerAutoConfiguration.class
})
@ConfigurationPropertiesScan
@EnableConfigurationProperties({ YamlPipelineProperties.class, MachineLadderProperties.class,
        com.di.streamnova.agent.shardplanner.ShardPlannerProperties.class })
public class StreamNovaApplication {

	public static void main(String[] args) {
		// Filter out "null" lines from stdout/stderr before Spring Boot starts
		// This prevents third-party libraries (like Apache Beam) from printing null messages
		if (System.getProperty("filter.null.output", "true").equals("true")) {
			System.setOut(new NullFilteringPrintStream(System.out, true));
			System.setErr(new NullFilteringPrintStream(System.err, true));
		}
		
		SpringApplication.run(StreamNovaApplication.class, args)
				.getBean(DataflowRunnerService.class)
				.runPipeline();
	}
}
