package com.example.test;

import lombok.*;

/**
 * 用户实体
 *
 * @author : snail
 * @date : 2022-03-21 17:52
 **/
@Getter
@Setter
@ToString
@NoArgsConstructor
@AllArgsConstructor
public class User {
    private String name;
    private String sex;
    private Integer age;
}
