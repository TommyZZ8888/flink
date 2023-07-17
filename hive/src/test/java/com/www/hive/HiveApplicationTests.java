package com.www.hive;

import com.www.hive.service.HiveService;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

@SpringBootTest
class HiveApplicationTests {

    @Autowired
    private HiveService hiveService;

    @Test
    void contextLoads() throws Exception {
        hiveService.init();

        hiveService.save();

        hiveService.init();
    }

}
