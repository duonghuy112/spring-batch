package com.huynguyen.springbatch.writer;

import com.huynguyen.springbatch.model.StudentCsv;
import org.springframework.batch.item.ItemWriter;
import org.springframework.stereotype.Component;

import java.util.List;

@Component
public class CsvItemWriter implements ItemWriter<StudentCsv> {

    @Override
    public void write(List<? extends StudentCsv> items) throws Exception {
        System.out.println("Inside Item Writer");
        items.stream().forEach(System.out::println);
    }
}
