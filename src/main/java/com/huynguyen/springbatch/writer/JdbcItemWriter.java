package com.huynguyen.springbatch.writer;

import com.huynguyen.springbatch.model.StudentJdbc;
import org.springframework.batch.item.ItemWriter;
import org.springframework.stereotype.Component;

import java.util.List;

@Component
public class JdbcItemWriter implements ItemWriter<StudentJdbc> {

    @Override
    public void write(List<? extends StudentJdbc> items) throws Exception {
        System.out.println("Inside Item Writer");
        items.stream().forEach(System.out::println);
    }
}
