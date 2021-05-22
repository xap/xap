/*
 * Copyright (c) 2008-2016, GigaSpaces Technologies, Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.j_spaces.core;

import com.gigaspaces.serialization.SmartExternalizable;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

/**
 * The SecurityContext class defines security info passed from the proxy to the space, and is used
 * by the security filters to validate access to space functions.
 *
 * @author Yechiel Fefer
 * @version 1.0
 **/
@Deprecated
@com.gigaspaces.api.InternalApi
public class SecurityContext
        implements SmartExternalizable

{
    private static final long serialVersionUID = 1L;

    private String _username;
    private String _password;
    private boolean _isEncrypted;
    // permissions can be set by the filter of operation SET_SECURITY, it is
    // a string that contains basic permissions (see below) which are checked in the client proxy before call the space
    private String _permissions;
    //m_ImplementorContext - implementation specific free context, must implement  Serializable
    private Object _implementorContext;

    // if set to true - means that this context was checked and initialized by the proxy
    private boolean _initialized;

    /**
     * Read permission.
     */
    public final static char PERMISSION_READ = 'R';  // read
    /**
     * Write/Take/Update/Replace permission.
     */
    public final static char PERMISSION_WRITE = 'W';  // write, take
    /**
     * Admin functions permission.
     */
    public final static char PERMISSION_ADMIN = 'A';  // admin functions
    /**
     * Task executions permission
     */
    public final static char PERMISSION_EXECUTE = 'E';  // execute

    /**
     * Default user name and password, when default constructor is used.
     *
     * @see #SecurityContext()
     */
    public final static String ANONYMOUS_USER = "ANONYMOUS";

    /**
     * The default constructor shall create a security context used for <code>ANONYMOUS_USER</code>
     * user account.
     */
    public SecurityContext() {
        this(ANONYMOUS_USER, ANONYMOUS_USER);
    }

    /**
     * Create new security context.
     *
     * @param username - User name shouldn't be null value.
     * @param password - Password shouldn't be null value.
     **/
    public SecurityContext(String username, String password) {
        setUsername(username);
        setPassword(password);
    }

    /**
     * Disable all operations to user represented by this security context.
     */
    public void disableAllOperations() {
        setPermissions("");
    }

    /**
     * {@inheritDoc}
     */
    public void writeExternal(ObjectOutput out)
            throws IOException {
        //m_UserId ?
        if (getUsername() != null) {
            out.writeBoolean(true);
            out.writeUTF(getUsername());
            if (_password != null) {
                out.writeBoolean(true);
                out.writeUTF(_password);
            } else
                out.writeBoolean(false);
        } else // user id = null
            out.writeBoolean(false);

        //m_Permissions ??
        if (_permissions != null) {
            out.writeBoolean(true);
            out.writeUTF(_permissions);
        } else
            out.writeBoolean(false);

        //m_ImplementorContext ??
        if (_implementorContext != null) {
            out.writeBoolean(true);
            out.writeObject(_implementorContext);
        } else
            out.writeBoolean(false);

        out.writeBoolean(_isEncrypted);
        out.writeBoolean(isInitialized());

    }

    /**
     * {@inheritDoc}
     */
    public void readExternal(ObjectInput in)
            throws IOException, ClassNotFoundException {
        //m_UserId ?
        if (in.readBoolean()) {
            _username = in.readUTF();
            if (in.readBoolean()) {
                _password = in.readUTF();
            }
        }

        //m_Permissions ??
        if (in.readBoolean()) {
            _permissions = in.readUTF();
        }

        //m_ImplementorContext ??
        if (in.readBoolean()) {
            _implementorContext = in.readObject();
        }

        _isEncrypted = in.readBoolean();
        setInitialized(in.readBoolean());
    }

    /**
     * Sets user name.
     *
     * @param username the user name
     */
    public void setUsername(String username) {
        this._username = username;
    }

    /**
     * Gets user name.
     *
     * @return user name
     */
    public String getUsername() {
        return _username;
    }

    /**
     * Sets password.
     *
     * @param pass the password
     */
    public void setPassword(String pass) {
        this._password = pass;
    }

    /**
     * Gets the password.
     *
     * @return the password
     */
    public String getPassword() {
        return _password;
    }

    /**
     * Set indicator if encrypted.
     *
     * @param isEncrypted <code>true</code> if encrypted
     */
    public void setEncrypted(boolean isEncrypted) {
        this._isEncrypted = isEncrypted;
    }

    /**
     * Check is encrypted.
     *
     * @return <code>true</code> if encrypted
     */
    public boolean isEncrypted() {
        return _isEncrypted;
    }

    /**
     * Set the permission.
     * <pre>
     * For example:
     * <code>
     *    SecurityContext context = ...;
     *    context.setPermissions("" + PERMISSION_WRITE + PERMISSION_READ); // read & write
     * permission
     * </code>
     * </pre>
     *
     * @param permissions the permission string
     */
    public void setPermissions(String permissions) {
        this._permissions = permissions;
    }

    /**
     * Gets the the permission <code>String</code>.
     *
     * @return the permission <code>String</code>
     * @see #setPermissions(String)
     */
    public String getPermissions() {
        return _permissions;
    }

    /**
     * Set implementation specific free context, must implement Serializable. This can be used to
     * attach a metadata to the context.
     *
     * @param implementorContext implementation specific free context
     */
    public void setImplementorContext(Object implementorContext) {
        this._implementorContext = implementorContext;
    }

    /**
     * Get the implementation specific free context.
     *
     * @return implementation specific free context
     * @see #setImplementorContext(Object)
     */
    public Object getImplementorContext() {
        return _implementorContext;
    }

    public boolean isInitialized() {
        return _initialized;
    }

    public void setInitialized(boolean initialized) {
        _initialized = initialized;
    }
}
