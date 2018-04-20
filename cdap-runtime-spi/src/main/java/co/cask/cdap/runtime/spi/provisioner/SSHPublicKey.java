/*
 * Copyright © 2018 Cask Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package co.cask.cdap.runtime.spi.provisioner;

/**
 * Contains information of a public key.
 */
public class SSHPublicKey {

  private final String user;
  private final String key;

  public SSHPublicKey(String user, String key) {
    this.user = user;
    this.key = key;
  }

  public String getUser() {
    return user;
  }

  public String getKey() {
    return key;
  }
}
