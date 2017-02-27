package com.zhaohanqing;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.SpringApplicationConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;
import org.springframework.test.context.web.WebAppConfiguration;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.RequestBuilder;
import org.springframework.test.web.servlet.ResultActions;
import org.springframework.test.web.servlet.request.MockMvcRequestBuilders;
import org.springframework.test.web.servlet.result.MockMvcResultMatchers;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;
import org.springframework.web.context.WebApplicationContext;


@RunWith(SpringJUnit4ClassRunner.class) 
@SpringApplicationConfiguration(classes = KafkaLagsMetricsApplication.class) 
@WebAppConfiguration 
public class KafkaLagsMetricsApplicationTests {
	
	private MockMvc mvc;
	
	@Autowired
	private WebApplicationContext wac; 
	
	
	@Before
	public void setUp() throws Exception {
		mvc = MockMvcBuilders.webAppContextSetup(this.wac).build();
	}

	@After
	public void tearDown() throws Exception {
		
	}
	
	
	@Test
	public void tesAPIkafkametricsinit() throws Exception {
		RequestBuilder requestBuilder = MockMvcRequestBuilders.get("/kafkametricsinit");
		ResultActions result= mvc.perform(requestBuilder);
		result.andExpect(MockMvcResultMatchers.status().isOk());
	}
	
	
	@Test
	public void testAPIgetMetrics() throws Exception{
		RequestBuilder requestBuilder = MockMvcRequestBuilders.get("/getMetrics");
		ResultActions result= mvc.perform(requestBuilder);
		result.andExpect(MockMvcResultMatchers.content().contentType("application/json;charset=UTF-8"));
	}
	
	
}
