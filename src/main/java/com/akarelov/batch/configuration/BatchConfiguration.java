package com.akarelov.batch.configuration;

import com.akarelov.batch.domain.Person;
import com.akarelov.batch.listener.JobCompletionNotificationListener;
import com.akarelov.batch.pocessor.PersonItemProcessor;
import com.jcraft.jsch.ChannelSftp;
import com.jcraft.jsch.JSch;
import com.jcraft.jsch.Session;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.batch.core.step.tasklet.TaskletStep;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.file.FlatFileItemReader;
import org.springframework.batch.item.file.builder.FlatFileItemReaderBuilder;
import org.springframework.batch.item.file.builder.FlatFileItemWriterBuilder;
import org.springframework.batch.item.file.mapping.BeanWrapperFieldSetMapper;
import org.springframework.batch.item.file.transform.BeanWrapperFieldExtractor;
import org.springframework.batch.item.file.transform.DelimitedLineAggregator;
import org.springframework.batch.repeat.RepeatStatus;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.ClassPathResource;
import org.springframework.core.io.FileSystemResource;

@Configuration
@EnableBatchProcessing
public class BatchConfiguration {
    public final JobBuilderFactory jobBuilderFactory;
    public final StepBuilderFactory stepBuilderFactory;
    private final String username;
    private final String password;
    private final String host;
    private final int port;
    private final String filePath;

    public BatchConfiguration(JobBuilderFactory jobBuilderFactory,
                              StepBuilderFactory stepBuilderFactory,
                              @Value("${username}") String username,
                              @Value("${password}") String password,
                              @Value("${host}") String host,
                              @Value("${port}") int port,
                              @Value("${filePath}") String filePath) {
        this.jobBuilderFactory = jobBuilderFactory;
        this.stepBuilderFactory = stepBuilderFactory;
        this.username = username;
        this.password = password;
        this.host = host;
        this.port = port;
        this.filePath = filePath;
    }

    @Bean
    public FlatFileItemReader<Person> reader() {//1
        return new FlatFileItemReaderBuilder<Person>()
                .name("personItemReader")
                .resource(new ClassPathResource("sample-data.csv"))
                .delimited()
                .names("firstName", "lastName")
                .fieldSetMapper(new BeanWrapperFieldSetMapper<Person>() {{
                    setTargetType(Person.class);
                }})
                .build();
    }

    @Bean
    public PersonItemProcessor processor() {//2
        return new PersonItemProcessor();
    }

    @Bean
    public FileSystemResource resource() {
        return new FileSystemResource(filePath);
    }

    @Bean
    public ItemWriter<Person> writer(FileSystemResource resource) {//3
        BeanWrapperFieldExtractor<Person> fieldExtractor = new BeanWrapperFieldExtractor<>();
        fieldExtractor.setNames(new String[]{"firstName", "lastName"});
        fieldExtractor.afterPropertiesSet();

        DelimitedLineAggregator<Person> lineAggregator = new DelimitedLineAggregator<>();
        lineAggregator.setDelimiter(",");
        lineAggregator.setFieldExtractor(fieldExtractor);

        return new FlatFileItemWriterBuilder<Person>()
                .name("customerPersonWriter")
                .resource(resource)
                .lineAggregator(lineAggregator)
                .build();
    }

    @Bean(name = "job10")
    public Job importUserJob(JobCompletionNotificationListener listener, @Qualifier(value = "step1") Step step1, @Qualifier(value = "step2") Step step2) {
        return jobBuilderFactory.get("importUserJob")
                .incrementer(new RunIdIncrementer())
                .listener(listener)
                .flow(step1)
                .next(step2)
                .end()
                .build();
    }

    @Bean(name = "step1")
    public Step step1(ItemWriter<Person> writer,
                      PersonItemProcessor processor,
                      FlatFileItemReader<Person> reader) {//4
        TaskletStep step1 = stepBuilderFactory.get("step1")
                .<Person, Person>chunk(10)
                .reader(reader)
                .processor(processor)
                .writer(writer)
                .build();
        return step1;
    }

    @Bean(name = "step2")
    public Step step2() {
        return stepBuilderFactory.get("step2")
                .tasklet((s1, s2) -> {
                    JSch jsch = new JSch();
                    Session session = jsch.getSession(username, host, port);
                    session.setPassword(password);
                    session.setConfig("StrictHostKeyChecking", "no");
//                    System.out.println("Establishing Connection...");
                    session.connect();
//                    System.out.println("Connection established.");
//                    System.out.println("Crating SFTP Channel.");
                    ChannelSftp sftpChannel = (ChannelSftp) session.openChannel("sftp");
                    sftpChannel.connect();
//                    System.out.println("SFTP Channel created.");
                    sftpChannel.put(filePath, "/tmp/output.txt");
                    session.disconnect();
                    return RepeatStatus.FINISHED;
                })
                .build();
    }
}