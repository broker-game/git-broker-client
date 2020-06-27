package io.github.jabrena.broker;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.BDDAssertions.then;
import static org.junit.jupiter.api.Assertions.assertThrows;

class AuthenticationTest {

    @Test
    public void given_Authentication_when_use_constructor_then_Ok() {

        String fullName = "user";
        String email = "user@my-email.com";
        String user = "xxx";
        String pasword = "yyy";

        Authentication authentication = new Authentication(fullName, email, user, pasword);

        then(authentication.getFullName()).isEqualTo(fullName);
        then(authentication.getEmail()).isEqualTo(email);
        then(authentication.getUser()).isEqualTo(user);
        then(authentication.getPassword()).isEqualTo(pasword);
    }

    @Test
    public void given_Authentication_when_use_mainConstructorWithNull_then_Ko() {

        Exception exception = assertThrows(IllegalArgumentException.class, () ->
            new Authentication(null, null, null, null));

        String expectedMessage = "fullName is marked non-null but is null";
        then(exception.getMessage()).isEqualTo(expectedMessage);
    }

}
