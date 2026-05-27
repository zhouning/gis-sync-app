package com.example.gis.backend.config;

import com.zaxxer.hikari.HikariDataSource;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.web.client.RestTemplate;

import javax.sql.DataSource;

/**
 * 配置类：
 * - 启用 GisProperties
 * - 显式声明两个 DataSource：default（@Primary，目标库）和 source（独立 bean，源库）
 *   一旦定义了任何 DataSource bean，Spring Boot 不再自动构造 spring.datasource，
 *   所以这里必须把目标库也手动建出来。
 * - 提供 RestTemplate 给 FlinkClient 用
 */
@Configuration
@EnableConfigurationProperties(GisProperties.class)
public class BeansConfig {

    @Bean
    @Primary
    public DataSource dataSource(
            @Value("${spring.datasource.url}") String url,
            @Value("${spring.datasource.username}") String username,
            @Value("${spring.datasource.password}") String password) {
        HikariDataSource ds = new HikariDataSource();
        ds.setJdbcUrl(url);
        ds.setUsername(username);
        ds.setPassword(password);
        ds.setDriverClassName("org.postgresql.Driver");
        ds.setMaximumPoolSize(10);
        ds.setPoolName("gis-dst-pool");
        return ds;
    }

    @Bean(name = "sourceDataSource", destroyMethod = "close")
    public DataSource sourceDataSource(GisProperties props) {
        HikariDataSource ds = new HikariDataSource();
        ds.setJdbcUrl(props.getSourceDb().getUrl());
        ds.setUsername(props.getSourceDb().getUsername());
        ds.setPassword(props.getSourceDb().getPassword());
        ds.setDriverClassName("org.postgresql.Driver");
        ds.setMaximumPoolSize(3);
        ds.setPoolName("gis-source-pool");
        return ds;
    }

    @Bean(name = "sourceJdbcTemplate")
    public JdbcTemplate sourceJdbcTemplate(DataSource sourceDataSource) {
        return new JdbcTemplate(sourceDataSource);
    }

    @Bean
    public RestTemplate restTemplate() {
        return new RestTemplate();
    }
}
