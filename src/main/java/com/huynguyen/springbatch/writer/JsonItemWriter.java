package com.huynguyen.springbatch.writer;

import com.huynguyen.springbatch.model.StudentJson;
import org.springframework.batch.item.ItemWriter;
import org.springframework.stereotype.Component;

import java.util.List;

@Component
public class JsonItemWriter implements ItemWriter<StudentJson> {

    @Override
    public void write(List<? extends StudentJson> items) throws Exception {
        System.out.println("Inside Item Writer");
        items.stream().forEach(System.out::println);
    }
}
