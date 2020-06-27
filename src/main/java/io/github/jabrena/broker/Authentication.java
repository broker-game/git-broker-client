package io.github.jabrena.broker;

import lombok.Data;
import lombok.NonNull;

@Data
public class Authentication {

    @NonNull
    private final String fullName;

    @NonNull
    private final String email;

    @NonNull
    private final String user;

    @NonNull
    private final String password;
}
