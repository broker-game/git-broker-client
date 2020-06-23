package io.github.jabrena.broker;

import lombok.Data;

@Data
public class Authentication {

    private final String fullName;
    private final String email;
    private final String user;
    private final String password;
}
