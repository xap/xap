package org.gigaspaces.cli;

/**
 * @author evgeny
 * @since 12.3
 */
public class CliCommandsSingleton {

  private final static CliCommandsSingleton instance = new CliCommandsSingleton();

  private String username;
  private String password;

  public static CliCommandsSingleton getInstance(){
    return instance;
  }

  private CliCommandsSingleton(){}

  public String getUsername() {
    return username;
  }

  public void setUsername(String username) {
    this.username = username;
  }

  public String getPassword() {
    return password;
  }

  public void setPassword(String password) {
    this.password = password;
  }
}
