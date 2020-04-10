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


package com.j_spaces.jms;

import com.gigaspaces.logger.Constants;
import com.j_spaces.core.EntrySerializationException;
import com.j_spaces.core.client.IReplicatable;
import com.j_spaces.core.client.MetaDataEntry;
import com.j_spaces.jms.utils.ConversionHelper;
import com.j_spaces.jms.utils.IMessageConverter;
import com.j_spaces.jms.utils.SerializationHelper;
import com.j_spaces.jms.utils.StringsUtils;
import com.j_spaces.kernel.ClassHelper;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectInputStream;
import java.io.ObjectOutput;
import java.io.ObjectOutputStream;
import java.util.Collections;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.jms.DeliveryMode;
import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageFormatException;
import javax.jms.MessageNotReadableException;
import javax.jms.MessageNotWriteableException;
import javax.jms.Session;
import javax.jms.TransactionRolledBackException;

public class GSMessageImpl extends MetaDataEntry implements Externalizable, IReplicatable// implements Message {
{
    private static final Integer[] constIntValues = {0, 1, 2, 3, 4, 5, 6, 7, 8, 9};

    private static final long serialVersionUID = 1L;

    /**
     * logger
     */
    final private static Logger _logger = LoggerFactory.getLogger(Constants.LOGGER_JMS);

    /**
     * <code>true</code> if the body is read-only. else the body is write-only
     */
    private boolean bodyRO = false;

    /**
     * <code>true</code> if the properties are read-only.
     */
    private boolean propertiesRO = false;


    /**
     * the time to live specified by the JMS client on send
     */
    private long ttl = Message.DEFAULT_TIME_TO_LIVE;

    /**
     * Routing index.
     */
    public String DestinationName;

    /**
     * The session this message belongs to.
     */
    protected transient GSSessionImpl session;

    /**
     * Message body
     */
    public Object Body;

    /**
     * JMSDestination header
     */
    public Destination JMSDestination;

    /**
     * JMSDeliveryMode header
     */
    public Integer JMSDeliveryMode;

    /**
     * JMSExpiration header
     */
    public Long JMSExpiration;

    /**
     * JMSPriority header
     */
    public Integer JMSPriority;

    /**
     * JMSMessageID header
     */
    public String JMSMessageID;

    /**
     * JMSTimestamp header
     */
    public Long JMSTimestamp;

    /**
     * JMSCorrelationID header
     */
    public Object JMSCorrelationID;

    /**
     * JMSReplyTo header
     */
    public Destination JMSReplyTo;

    /**
     * JMSType header
     */
    public String JMSType;

    /**
     * JMSRedelivered header
     */
    public Boolean JMSRedelivered;

    /**
     * Message properties
     */
    public Map<String, Object> Properties;


    /**
     * Indexed fields
     */
    protected static String[] indexedFields = {"DestinationName"};

    /**
     * A simple message carries a simple empty body.
     */
    public static final String SIMPLE = "Simple";

    /**
     * A text message carries a String body.
     */
    public static final String TEXT = "Text";

    /**
     * An object message carries a serializable object.
     */
    public static final String OBJECT = "Object";

    /**
     * A map message carries an hashtable.
     */
    public static final String MAP = "Map";

    /**
     * A stream message carries a bytes stream.
     */
    public static final String STREAM = "Stream";

    /**
     * A bytes message carries an array of bytes.
     */
    public static final String BYTES = "Bytes";


    // header names
    public static final String BODY_STR_NAME = "message_body";
    public static final String JMS_DESTINATION = "JMSDestination";
    public static final String JMS_DELIVERY_MODE = "JMSDeliveryMode";
    public static final String JMS_EXPIRATION = "JMSExpiration";
    public static final String JMS_PRIORITY = "JMSPriority";
    public static final String JMS_MESSAGE_ID = "JMSMessageID";
    public static final String JMS_TIMESTAMP = "JMSTimestamp";
    public static final String JMS_CORRELATION_ID = "JMSCorrelationID";
    public static final String JMS_REPLY_TO = "JMSReplyTo";
    public static final String JMS_TYPE = "JMSType";
    public static final String JMS_REDELIVERED = "JMSRedelivered";
    public static final String JMS_GSPRODUCER_KEY_PROP_NAME = "JMS_GSProducerKey";
    public static final String JMS_GSTTL_KEY_PROP_NAME = "JMS_GSTTLKey";
    public static final String JMSX_GROUPID = "JMSXGroupID";
    public static final String JMSX_GROUPSEQ = "JMSXGroupSeq";
    public static final String JMSX_USERID = "JMSXUserID";
    public static final String PROPERTIES_STR_NAME = "message_client_properties";
    public static final String JMS_GSCONNECTION_KEY_NAME = "JMS_GSConnectionKey";
    public static final String JMS_GSCONVERTER = "JMS_GSMessageConverter";


    /**
     * Used internally only
     */
    public GSMessageImpl() {
        super();
        setFifo(true);
        setNOWriteLeaseMode(true);
        makeTransient();
    }


    /**
     * Constructs a new GSMessageImpl instance.
     *
     * @param session The session
     * @param type    The message type
     */
    public GSMessageImpl(GSSessionImpl session, String type) throws JMSException {
        this();
        setSession(session);
        setJMSType(type);
        Properties = new HashMap<String, Object>(0);
    }


    public static String[] __getSpaceIndexedFields() {
        return indexedFields;
    }


    public void setRoutingIndexes(String[] indexes) {
        indexedFields = indexes;
    }


    GSSessionImpl getSession() {
        return session;
    }


    /**
     * sets the time to live specified by the JMS client on send
     *
     * @param ttl the time to live value
     */
    void setTTL(long ttl) {
        this.ttl = ttl;
    }

    /**
     * returns the time to live specified by the JMS client on send
     *
     * @return the time to live value
     */
    long getTTL() {
        return ttl;
    }


    void setSession(GSSessionImpl session) {
        this.session = session;
    }


    void setBodyReadOnly(boolean readOnly) {
        bodyRO = readOnly;
    }


    void setPropertiesReadOnly(boolean readOnly) {
        propertiesRO = readOnly;
    }


    /**
     * @see Message#acknowledge()
     */
    public void acknowledge() throws JMSException {
        // throw exception if the session is close
        session.ensureOpen();

        if (session.m_acknowledgeMode != Session.CLIENT_ACKNOWLEDGE) {
            if (_logger.isDebugEnabled()) {
                _logger.debug("GSMessageImpl.acknowledge(): Session's acknowledge" +
                        " mode != CLIENT_ACKNOWLEDGE - ignoring." + this.JMSMessageID);
            }
            return;
        }
        if (_logger.isDebugEnabled()) {
            _logger.debug("GSMessageImpl.acknowledge(): " + this.JMSMessageID);
        }

        try {
            session.acknowledge();
        } catch (CommitFailedException e) {
            // Happens only for QUEUE!
            if (_logger.isErrorEnabled()) {
                _logger.error(
                        "GSMessageImpl.acknowledge(): Failed to acknowledge consumed messages: " + e.orig);
            }

            try {
                session.recoverMessages();
            } catch (RollbackFailedException e1) {
                if (_logger.isErrorEnabled()) {
                    _logger.error(
                            "GSMessageImpl.acknowledge(): Failed to recover messages of transaction " +
                                    session.getTransaction() + e1.orig);
                }
            }

            TransactionRolledBackException re = new TransactionRolledBackException(
                    "Failed to acknowledge consumed messages. Transaction rolled back.");
            re.setLinkedException(e.orig);
            throw re;
        } finally {
            if (session.m_isQueue && !session.isAutoAck()) {
                try {
                    session.renewTransaction();
                } catch (TransactionCreateException e) {
                    if (_logger.isErrorEnabled()) {
                        _logger.error(
                                "GSMessageImpl.acknowledge(): Failed to renew transaction");
                    }
                    JMSException je = new JMSException("Failed to renew transaction");
                    je.setLinkedException(e.orig);
                    throw je;
                }
            }
        }
    }


    public void setDestinationName(String name) {
        DestinationName = name;
    }


    public String getDestinationName() {
        return DestinationName;
    }


    /**
     * @see Message#clearBody()
     */
    public void clearBody() throws JMSException {
        Body = null;
        setBodyReadOnly(false);
    }


    /**
     * @see Message#setJMSCorrelationID(String)
     */
    public void setJMSCorrelationID(String value) throws JMSException {
        JMSCorrelationID = value;
    }


    /**
     * @see Message#getJMSCorrelationID()
     */
    public String getJMSCorrelationID() throws JMSException {
        if (JMSCorrelationID != null) {
            if (JMSCorrelationID instanceof String) {
                return (String) JMSCorrelationID;
            }
            throw new JMSException("JMSCorrelationID is an array of bytes");
        }
        return null;
    }


    /**
     * @see Message#setJMSCorrelationIDAsBytes(byte[])
     */
    public void setJMSCorrelationIDAsBytes(byte[] value) throws JMSException {
        if (value != null) {
            JMSCorrelationID = new byte[value.length];
            System.arraycopy(value, 0, JMSCorrelationID, 0, value.length);
        } else {
            JMSCorrelationID = value;
        }
    }


    /**
     * @see Message#getJMSCorrelationIDAsBytes()
     */
    public byte[] getJMSCorrelationIDAsBytes() throws JMSException {
        if (JMSCorrelationID != null) {
            if (JMSCorrelationID instanceof byte[]) {
                byte[] correlatiomId = (byte[]) JMSCorrelationID;
                byte[] id = new byte[correlatiomId.length];
                System.arraycopy(correlatiomId, 0, id, 0, correlatiomId.length);
                return id;
            }
            throw new JMSException("JMSCorrelationID is a String");
        }
        return null;
    }


    /**
     * @see Message#setJMSDeliveryMode(int)
     */
    public void setJMSDeliveryMode(int value) throws JMSException {
        if (value == DeliveryMode.NON_PERSISTENT) {
            makeTransient();
        } else if (value == DeliveryMode.PERSISTENT) {
            makePersistent();
        } else {
            throw new JMSException("Delivery Mode of " + value + " is not valid");
        }
        JMSDeliveryMode = constIntValues[value];
    }


    /**
     * @see Message#getJMSDeliveryMode()
     */
    public int getJMSDeliveryMode() throws JMSException {
        if (JMSDeliveryMode != null) {
            return JMSDeliveryMode.intValue();
        }
        return Message.DEFAULT_DELIVERY_MODE;
    }


    /**
     * @see Message#setJMSDestination(Destination)
     */
    public void setJMSDestination(Destination value) throws JMSException {
        JMSDestination = value;
        setDestinationName(value != null ? value.toString() : null);
    }


    /**
     * @see Message#getJMSDestination()
     */
    public Destination getJMSDestination() throws JMSException {
        return JMSDestination;
    }


    /**
     * @see Message#setJMSExpiration(long)
     */
    public void setJMSExpiration(long value) throws JMSException {
        JMSExpiration = Long.valueOf(value);
    }


    /**
     * @see Message#getJMSExpiration()
     */
    public long getJMSExpiration() throws JMSException {
        if (JMSExpiration != null) {
            return JMSExpiration.longValue();
        }
        //return Long.MAX_VALUE;
        return Message.DEFAULT_TIME_TO_LIVE;
    }


    /**
     * @see Message#setJMSMessageID(String)
     */
    public void setJMSMessageID(String value) throws JMSException {
        JMSMessageID = value;
    }


    /**
     * @see Message#getJMSMessageID()
     */
    public String getJMSMessageID() throws JMSException {
        return JMSMessageID;
    }


    /**
     * @see Message#setJMSPriority(int)
     */
    public void setJMSPriority(int priority) throws JMSException {
        if (0 <= priority && priority <= 9) {
            JMSPriority = constIntValues[priority];
        } else {
            throw new JMSException("Priority of " + priority + " is not valid");
        }
    }


    /**
     * @see Message#getJMSPriority()
     */
    public int getJMSPriority() throws JMSException {
        if (JMSPriority != null) {
            return JMSPriority.intValue();
        }
        return Message.DEFAULT_PRIORITY;
    }


    /**
     * @see Message#setJMSRedelivered(boolean)
     */
    public void setJMSRedelivered(boolean redelivered) throws JMSException {
        JMSRedelivered = redelivered ? Boolean.TRUE : Boolean.FALSE;
    }


    /**
     * @see Message#getJMSRedelivered()
     */
    public boolean getJMSRedelivered() throws JMSException {
        if (JMSRedelivered != null) {
            return JMSRedelivered.booleanValue();
        }
        return false;
    }


    /**
     * @see Message#setJMSReplyTo(Destination)
     */
    public void setJMSReplyTo(Destination destination) throws JMSException {
        JMSReplyTo = destination;
    }


    /**
     * @see Message#getJMSReplyTo()
     */
    public Destination getJMSReplyTo() throws JMSException {
        return JMSReplyTo;
    }


    /**
     * @see Message#setJMSTimestamp(long)
     */
    public void setJMSTimestamp(long timestamp) throws JMSException {
        JMSTimestamp = Long.valueOf(timestamp);
    }


    /**
     * @see Message#getJMSTimestamp()
     */
    public long getJMSTimestamp() throws JMSException {
        if (JMSTimestamp != null) {
            return JMSTimestamp.longValue();
        }
        // TODO: -1?
        return 0L; // must be set or not used
    }


    /**
     * @see Message#setJMSType(String)
     */
    public void setJMSType(String type) throws JMSException {
        if (type != null &&
                (type.equals(SIMPLE) ||
                        type.equals(TEXT) ||
                        type.equals(OBJECT) ||
                        type.equals(MAP) ||
                        type.equals(STREAM) ||
                        type.equals(BYTES))) {
            JMSType = type;
        } else {
            throw new JMSException("JMS type of " + type + "is not valid");
        }
    }


    /**
     * @see Message#getJMSType()
     */
    public String getJMSType() throws JMSException {
        if (JMSType != null) {
            return JMSType;
        }
        return SIMPLE;
    }

    /**
     * @see Message#clearProperties()
     */
    public void clearProperties() throws JMSException {
        Properties.clear();
        setPropertiesReadOnly(false);
    }


    /**
     * @see Message#getPropertyNames()
     */
    public Enumeration getPropertyNames() throws JMSException {
        String name;
        HashSet<String> propNames = new HashSet<String>();
        Iterator<String> propNamesIter = Properties.keySet().iterator();
        while (propNamesIter.hasNext()) {
            name = propNamesIter.next();
            if (!name.startsWith("JMS_GS") && !name.startsWith("JMSX")) {
                propNames.add(name);
            }
        }
        return Collections.enumeration(propNames);
    }


    /**
     * @see Message#propertyExists(String)
     */
    public boolean propertyExists(String name) throws JMSException {
        return Properties.containsKey(name);
    }


    protected void checkPropertiesReadOnly() throws JMSException {
        if (propertiesRO) {
            throw new MessageNotWriteableException("Can't change message properties as they "
                    + "are read-only.");
        }
    }


    protected final void checkBodyReadOnly() throws MessageNotWriteableException {
        if (bodyRO) {
            throw new MessageNotWriteableException("Can't change message body as it "
                    + "is read-only.");
        }
    }


    protected final void checkBodyWriteOnly() throws MessageNotReadableException {
        if (!bodyRO) {
            throw new MessageNotReadableException("Can't read a value as the"
                    + " message body is write-only.");
        }
    }


    /**
     * @see Message#getBooleanProperty(String)
     */
    public boolean getBooleanProperty(String name) throws JMSException {
        Object value = Properties.get(name);
        return ConversionHelper.getBoolean(value);
    }


    /**
     * @see Message#getByteProperty(String)
     */
    public byte getByteProperty(String name) throws JMSException {
        Object value = Properties.get(name);
        return ConversionHelper.getByte(value);
    }


    /**
     * @see Message#getDoubleProperty(String)
     */
    public double getDoubleProperty(String name) throws JMSException {
        Object value = Properties.get(name);
        return ConversionHelper.getDouble(value);
    }


    /**
     * @see Message#getFloatProperty(String)
     */
    public float getFloatProperty(String name) throws JMSException {
        Object value = Properties.get(name);
        return ConversionHelper.getFloat(value);
    }


    /**
     * @see Message#getIntProperty(String)
     */
    public int getIntProperty(String name) throws JMSException {
        Object value = Properties.get(name);
        return ConversionHelper.getInt(value);
    }


    /**
     * @see Message#getLongProperty(String)
     */
    public long getLongProperty(String name) throws JMSException {
        Object value = Properties.get(name);
        return ConversionHelper.getLong(value);
    }


    /**
     * @see Message#getObjectProperty(String)
     */
    public Object getObjectProperty(String name) throws JMSException {
        return Properties.get(name);
    }


    /**
     * @see Message#getShortProperty(String)
     */
    public short getShortProperty(String name) throws JMSException {
        Object value = Properties.get(name);
        return ConversionHelper.getShort(value);
    }


    /**
     * @see Message#getStringProperty(String)
     */
    public String getStringProperty(String name) throws JMSException {
        Object value = Properties.get(name);
        return ConversionHelper.getString(value);
    }


    /**
     * @see Message#setBooleanProperty(String, boolean)
     */
    public void setBooleanProperty(String name, boolean value) throws JMSException {
        checkPropertiesReadOnly();
        Boolean i = value ? Boolean.TRUE : Boolean.FALSE;
        checkProperty(name, i);
        Properties.put(name, i);
    }


    /**
     * @see Message#setByteProperty(String, byte)
     */
    public void setByteProperty(String name, byte value) throws JMSException {
        checkPropertiesReadOnly();
        Byte i = Byte.valueOf(value);
        checkProperty(name, i);
        Properties.put(name, i);
    }


    /**
     * @see Message#setDoubleProperty(String, double)
     */
    public void setDoubleProperty(String name, double value) throws JMSException {
        checkPropertiesReadOnly();
        Double i = Double.valueOf(value);
        checkProperty(name, i);
        Properties.put(name, i);
    }


    /**
     * @see Message#setFloatProperty(String, float)
     */
    public void setFloatProperty(String name, float value) throws JMSException {
        checkPropertiesReadOnly();
        Float i = Float.valueOf(value);
        checkProperty(name, i);
        Properties.put(name, i);
    }


    /**
     * @see Message#setIntProperty(String, int)
     */
    public void setIntProperty(String name, int value) throws JMSException {
        checkPropertiesReadOnly();
        Integer i = Integer.valueOf(value);
        checkProperty(name, i);
        Properties.put(name, i);
    }


    /**
     * @see Message#setLongProperty(String, long)
     */
    public void setLongProperty(String name, long value) throws JMSException {
        checkPropertiesReadOnly();
        Long i = Long.valueOf(value);
        checkProperty(name, i);
        Properties.put(name, i);
    }


    /**
     * @see Message#setObjectProperty(String, Object)
     */
    public void setObjectProperty(String name, Object value) throws JMSException {
        checkPropertiesReadOnly();
        checkProperty(name, value);
        if (value instanceof Boolean ||
                value instanceof Byte ||
                value instanceof Short ||
                value instanceof Integer ||
                value instanceof Long ||
                value instanceof Float ||
                value instanceof Double ||
                value instanceof String ||
                value == null) {
            Properties.put(name, value);
        } else if (value instanceof IMessageConverter) {
            if (name.equals(GSMessageImpl.JMS_GSCONVERTER)) {
                Properties.put(name, value);
            } else {
                throw new MessageFormatException("Property value of type IMessageConverter" +
                        " can be set only for the property " + GSMessageImpl.JMS_GSCONVERTER);
            }
        } else {
            throw new MessageFormatException(
                    "Invalid object type: Message.setObjectProperty() does not support objects of " +
                            "type " + value.getClass().getName());
        }
    }


    /**
     * @see Message#setShortProperty(String, short)
     */
    public void setShortProperty(String name, short value) throws JMSException {
        checkPropertiesReadOnly();
        Short i = Short.valueOf(value);
        checkProperty(name, i);
        Properties.put(name, i);
    }


    /**
     * @see Message#setStringProperty(String, String)
     */
    public void setStringProperty(String name, String value) throws JMSException {
        checkPropertiesReadOnly();
        checkProperty(name, value);
        Properties.put(name, value);
    }


    /**
     * @see Object#equals(Object)
     */
    public boolean equals(Object o) {
        if (o == null) {
            return false;
        } else if (this == o) {
            return true;
        } else if (o instanceof GSMessageImpl) {
            if (JMSMessageID != null) {
                return JMSMessageID.equals(((GSMessageImpl) o).JMSMessageID);
            }
            return ((GSMessageImpl) o).JMSMessageID == null;
        }
        return false;
    }


    /**
     * @see Object#hashCode()
     */
    @Override
    public int hashCode() {
        if (JMSMessageID != null) {
            return JMSMessageID.hashCode();
        }
        return -1;
    }


    /**
     * @see Object#toString()
     */
    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("\n");
        sb.append(this.getClass().getName());

        // body
        sb.append(":\n\tBody:\n\t\t");
        sb.append(Body);

        // headers
        sb.append("\n\tHeaders:");
        sb.append("\n\t\tJMSDestination:\t\t");
        sb.append(JMSDestination);
        sb.append("\n\t\tJMSDeliveryMode:\t");
        sb.append(JMSDeliveryMode);
        sb.append("\n\t\tJMSExpiration:\t\t");
        sb.append(JMSExpiration);
        sb.append("\n\t\tJMSPriority:\t\t");
        sb.append(JMSPriority);
        sb.append("\n\t\tJMSMessageID:\t\t");
        sb.append(JMSMessageID);
        sb.append("\n\t\tJMSTimestamp:\t\t");
        sb.append(JMSTimestamp);
        sb.append("\n\t\tJMSCorrelationID:\t");
        sb.append(JMSCorrelationID);
        sb.append("\n\t\tJMSReplyTo:\t\t");
        sb.append(JMSReplyTo);
        sb.append("\n\t\tJMSType:\t\t");
        sb.append(JMSType);
        sb.append("\n\t\tJMSRedelivered:\t\t");
        sb.append(JMSRedelivered);

        // properties
        sb.append("\n\tProperties:\n\t\t");
        if (Properties != null) {
            sb.append(Properties.toString());
        }

        return sb.toString();
    }

    /**
     * Creates a copy of this message.
     *
     * @return the copy of this message.
     */
    GSMessageImpl duplicate() throws JMSException {
        GSMessageImpl dup = new GSMessageImpl();
        copyTo(dup);
        return dup;
    }

    /**
     * Copies the content of this message into dup.
     *
     * @param dup the message to copy into.
     */
    protected void copyTo(GSMessageImpl dup) {
        dup.setFifo(this.isFifo());
        if (this.isTransient()) {
            dup.makeTransient();
        } else {
            dup.makePersistent();
        }
        dup.setNOWriteLeaseMode(this.isNOWriteLeaseMode());

        dup.bodyRO = this.bodyRO;
        dup.propertiesRO = this.propertiesRO;
        dup.session = this.session;
        dup.DestinationName = this.DestinationName;
        dup.ttl = this.ttl;

        // copy the headers
        dup.JMSDestination = this.JMSDestination;  //
        dup.JMSDeliveryMode = this.JMSDeliveryMode;
        dup.JMSExpiration = this.JMSExpiration;
        dup.JMSPriority = this.JMSPriority;
        dup.JMSMessageID = this.JMSMessageID;
        dup.JMSTimestamp = this.JMSTimestamp;
        dup.JMSCorrelationID = this.JMSCorrelationID;
        dup.JMSReplyTo = this.JMSReplyTo; //
        dup.JMSType = this.JMSType;
        dup.JMSRedelivered = this.JMSRedelivered;

        // copy the body
        dup.Body = cloneBody();

        // copy the properties
        dup.Properties = cloneProperties();
    }


    /**
     * Returns a clone of the object.
     *
     * @return a clone of the object.
     */
    protected Object cloneObject(Object obj) {
        if (obj == null) {
            return null;
        }
        if (obj instanceof String ||
                obj instanceof Boolean ||
                obj instanceof Byte ||
                obj instanceof Short ||
                obj instanceof Character ||
                obj instanceof Integer ||
                obj instanceof Long ||
                obj instanceof Float ||
                obj instanceof Double) {
            return obj;
        }
        if (obj instanceof byte[]) {
            byte[] body = (byte[]) obj;
            byte[] data = new byte[body.length];
            System.arraycopy(body, 0, data, 0, body.length);
            return data;
        }
        return SerializationHelper.deepClone(obj);
    }


    /**
     * Returns a clone of the body.
     *
     * @return a clone of the body.
     */
    protected Object cloneBody() {
        return cloneObject(Body);
    }


    /**
     * Returns a clone of the properties map.
     *
     * @return a clone of the properties map.
     */
    HashMap<String, Object> cloneProperties() {
        if (Properties == null) {
            return null;
        }
        HashMap<String, Object> props = new HashMap<String, Object>(Properties.size());
        props.putAll(Properties);
        return props;
    }


    /**
     * Sets the properties HashMap.
     *
     * @param props the properties HashMap
     */
    public void setProperties(HashMap<String, Object> props) {
        Properties = props;
    }

//	public static GSMessageImpl getMessageTemplate()
//	{
//		GSMessageImpl template = new GSMessageImpl();
//		template.setProperties(null);
//		return template;
//	}


    /**
     * Method validates a new property. It checks if it is a proper JMS_GS, JMSX, JMS prop types,
     * before we can actually set it.
     *
     * @param name The property name.
     * @throws MessageFormatException       If the property m_type is invalid.
     * @throws MessageNotWriteableException If the message is read-only.
     * @throws JMSException                 If the name is invalid.
     * @throws IllegalArgumentException     If the name string is null or empty.
     */
    private void checkProperty(String name, Object val) throws JMSException {
        //if(!toValidate)
        //	return;
        //verifyPropertyName(name);
        if (name == null) {
            throw new JMSException("The name of a property must not be null.");
        }

        if (name.equals("")) {
            throw new JMSException("The name of a property must not be an empty String.");
        }

        char[] chars = name.toCharArray();
        if (chars.length == 0) {
            throw new JMSException("zero-length name is not a valid " +
                    "property name");
        }
        if (!Character.isJavaIdentifierStart(chars[0])) {
            throw new JMSException("Property name " + name + " is not a valid " +
                    "property name");
        }
        for (int i = 1; i < chars.length; ++i) {
            if (!Character.isJavaIdentifierPart(chars[i])) {
                throw new JMSException("Property name" + name + " is not a valid " +
                        "property name");
            }
        }
        //		for (int i = 0; i < RESERVED_WORDS.length; ++i) {
        //			if (name.equalsIgnoreCase(RESERVED_WORDS[i])) {
        //				throw new JMSException("name=" + name + " is a reserved " +
        //					"word; it cannot be used as a " +
        //					"property name");
        //			}
        //		}

        if (name.startsWith("JMSX")) //JMS msg properties, defined by the client
        {
            //			the only JMSX valid props to be set by the client are:
            boolean found = false;
            for (int i = 0; i < GSSessionImpl.JMSX_CLIENT_NAMES.length; ++i) {
                Object[] types = GSSessionImpl.JMSX_CLIENT_NAMES[i];
                if (types[0].equals(name)) {
                    if (val == null) {
                        throw new MessageFormatException("Property=" + name +
                                " may not be null");
                    }
                    Class type = (Class) types[1];
                    //Verifies that the only allowed type for JMSXGroupID is an int.
                    if (!type.equals(val.getClass())) {
                        throw new MessageFormatException("Expected type " + type.getName() +
                                " for property" + name + ", but got type " +
                                val.getClass().getName());
                    }
                    if (name.equals(GSMessageImpl.JMSX_GROUPSEQ) &&
                            ((Integer) val).intValue() <= 0) {
                        throw new JMSException(GSMessageImpl.JMSX_GROUPSEQ + " must have a value > 0");
                    }
                    found = true;
                    break;
                }
            }
            if (!found) {
                throw new JMSException("The property name '" + name + "' is not a valid java identifier." +
                        "Property names with prefix 'JMSX' are"
                        + " reserved for provider usage and cannot be set by clients.");
            }
        }

        if (name.startsWith("JMSX"))//JMS msg properties, defined by the client
        {
            //the only JMSX valid props to be set by the client are:
            if (name.equals(GSMessageImpl.JMSX_GROUPID)) {
                if (!ClassHelper.getPrimitiveName(val.getClass()).equalsIgnoreCase("int")) {
                    //Verifies that the only allowed type for JMSXGroupID is an int.

                }
                return;
            }

            if (name.equals(GSMessageImpl.JMSX_GROUPSEQ))
                return;

            throw new JMSException("The property name '" + name + "' is not a valid java identifier." +
                    "Property names with prefix 'JMSX' are"
                    + " reserved for provider usage.");
        }

        if (name.startsWith("JMS_GS"))//GigaSpaces specific msg properties
        {
            //if(name.equals( GSMessageImpl.JMS_GSPRODUCER_KEY_PROP_NAME ))
            if (name.equals(GSMessageImpl.JMS_GSCONNECTION_KEY_NAME))
                return;

            if (name.equals(GSMessageImpl.JMS_GSCONVERTER)) {
                if (val == null || val instanceof IMessageConverter)
                    return;

                throw new MessageFormatException("The value of property " + name +
                        " should be of type IMessageConverter.");
            }
            /*else if(name.equals( GSMessageImpl.JMS_GSEXTERNAL_ENTRY_UID_PROP_NAME ))
                      return;*/
            throw new JMSException("The property name '" + name + "' is not a valid java identifier." +
                    "Property names with prefix 'JMS_GS' are"
                    + "reserved for JMS Message usage and cannot be set by clients.");
        }

        if (name.startsWith("JMS"))//JMSMessage properties
            throw new JMSException("The property name '" + name + "' is not a valid java identifier." +
                    "Property names with prefix 'JMS' are"
                    + " reserved for JMS Message usage and cannot be set by clients.");

        //we should also avoid using java primitive types or other reserved words
        if (StringsUtils.isValidJavaIdentifier(name) == false)
            throw new JMSException("The property name '" + name + "' is not a valid java identifier.");

        if (GSSessionImpl.reservedSelectorIdentifiers.contains(name.toUpperCase()))
            throw new JMSException("The property name '" + name + "' is reserved due to message selector syntax.");
    }


    private final static class BitMap {
        private static final int DESTINATION_NAME_BIT = 0x00000001;
        private static final int JMS_DESTINATION_BIT = 0x00000002;
        private static final int JMS_DELIVERY_MODE_BIT = 0x00000004;
        private static final int JMS_EXPIRATION_BIT = 0x00000008;
        private static final int JMS_PRIORITY_BIT = 0x00000010;
        private static final int JMS_MESSAGE_ID_BIT = 0x00000020;
        private static final int JMS_TIMESTAMP_BIT = 0x00000040;
        private static final int JMS_CORRELATION_ID_BIT = 0x00000080;
        private static final int JMS_REPLY_TO_BIT = 0x00000100;
        private static final int JMS_TYPE_BIT = 0x00000200;
        private static final int JMS_REDELIVERED_BIT = 0x00000400;
        private static final int BODY_BIT = 0x00000800;
        private static final int PROPERTIES_BIT = 0x00001000;

    }


    /**
     * These bytes indicate a bit index for all the primitive types written and then read from the
     * stream. This way we avoid using writeObject() and readObject() in case of primitive types,
     * which is very expensive.
     */
    private final static byte OBJECT_BIT = 0;
    private final static byte INTEGER_BIT = 1;
    private final static byte DOUBLE_BIT = 2;
    private final static byte FLOAT_BIT = 3;
    private final static byte LONG_BIT = 4;
    private final static byte STRING_BIT = 5;
    private final static byte SHORT_BIT = 6;
    private final static byte BOOLEAN_BIT = 7;
    private final static byte CHAR_BIT = 8;
    private final static byte BYTE_BIT = 9;


    /**
     * Creates a map of what is null and what is not
     */
    private int buildFlags() {
        int flags = 0;

        if (DestinationName != null) {
            flags |= BitMap.DESTINATION_NAME_BIT;
        }
        if (JMSDestination != null) {
            flags |= BitMap.JMS_DESTINATION_BIT;
        }
        if (JMSDeliveryMode != null) {
            flags |= BitMap.JMS_DELIVERY_MODE_BIT;
        }
        if (JMSExpiration != null) {
            flags |= BitMap.JMS_EXPIRATION_BIT;
        }
        if (JMSPriority != null) {
            flags |= BitMap.JMS_PRIORITY_BIT;
        }
        if (JMSMessageID != null) {
            flags |= BitMap.JMS_MESSAGE_ID_BIT;
        }
        if (JMSTimestamp != null) {
            flags |= BitMap.JMS_TIMESTAMP_BIT;
        }
        if (JMSCorrelationID != null) {
            flags |= BitMap.JMS_CORRELATION_ID_BIT;
        }
        if (JMSReplyTo != null) {
            flags |= BitMap.JMS_REPLY_TO_BIT;
        }
        if (JMSType != null) {
            flags |= BitMap.JMS_TYPE_BIT;
        }
        if (JMSRedelivered != null) {
            flags |= BitMap.JMS_REDELIVERED_BIT;
        }
        if (Body != null) {
            flags |= BitMap.BODY_BIT;
        }
        if (Properties != null) {
            flags |= BitMap.PROPERTIES_BIT;
        }
        return flags;
    }


    /**
     * @see Externalizable#writeExternal(ObjectOutput)
     */
    public void writeExternal(ObjectOutput out) throws IOException {
        try {
            super._writeExternal(out);

            int flags = buildFlags();
            out.writeInt(flags);

            if (DestinationName != null) {
                out.writeUTF(DestinationName);
            }
            if (JMSDestination != null) {
                if (out instanceof ObjectOutputStream) {
                    ((ObjectOutputStream) out).writeUnshared(JMSDestination);
                } else {
                    out.writeObject(JMSDestination);
                }
            }
            if (JMSDeliveryMode != null) {
                out.writeInt(JMSDeliveryMode);
            }
            if (JMSExpiration != null) {
                out.writeLong(JMSExpiration);
            }
            if (JMSPriority != null) {
                out.writeInt(JMSPriority);
            }
            if (JMSMessageID != null) {
                out.writeUTF(JMSMessageID);
            }
            if (JMSTimestamp != null) {
                out.writeLong(JMSTimestamp);
            }
            if (JMSCorrelationID != null) {
                if (JMSCorrelationID instanceof byte[]) {
                    byte[] correlationId = (byte[]) JMSCorrelationID;
                    out.writeByte(BYTE_BIT);
                    out.writeInt(correlationId.length);
                    out.write(correlationId);
                } else {
                    out.writeByte(STRING_BIT);
                    out.writeUTF(JMSCorrelationID.toString());
                }
            }
            if (JMSReplyTo != null) {
                if (out instanceof ObjectOutputStream) {
                    ((ObjectOutputStream) out).writeUnshared(JMSReplyTo);
                } else {
                    out.writeObject(JMSReplyTo);
                }
            }
            if (JMSType != null) {
                out.writeUTF(JMSType);
            }
            if (JMSRedelivered != null) {
                out.writeBoolean(JMSRedelivered);
            }
            if (Body != null) {
                if (out instanceof ObjectOutputStream) {
                    ((ObjectOutputStream) out).writeUnshared(Body);
                } else {
                    out.writeObject(Body);
                }
            }
            if (Properties != null) {
                out.writeInt(Properties.size());
                Iterator<String> i = Properties.keySet().iterator();
                while (i.hasNext()) {
                    String name = i.next();
                    out.writeUTF(name);
                    writePrimitiveObject(out, Properties.get(name), name);
                }
            }
        } catch (Exception ex) {
            if (ex instanceof EntrySerializationException)
                throw (EntrySerializationException) ex;

            //String className = m_ClassName != null ? m_ClassName : ".";
            throw new EntrySerializationException("Failed to serialize JMS message: " + toString(), ex);
        }
    }


    /**
     * @see Externalizable#readExternal(ObjectInput)
     */
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        try {
            super._readExternal(in);

            int flags = in.readInt();

            if ((flags & BitMap.DESTINATION_NAME_BIT) != 0) {
                this.DestinationName = in.readUTF();
            }
            if ((flags & BitMap.JMS_DESTINATION_BIT) != 0) {
                if (in instanceof ObjectInputStream) {
                    JMSDestination = (Destination) ((ObjectInputStream) in).readUnshared();
                } else {
                    JMSDestination = (Destination) in.readObject();
                }
            }
            if ((flags & BitMap.JMS_DELIVERY_MODE_BIT) != 0) {
                JMSDeliveryMode = in.readInt();
            }
            if ((flags & BitMap.JMS_EXPIRATION_BIT) != 0) {
                JMSExpiration = in.readLong();
            }
            if ((flags & BitMap.JMS_PRIORITY_BIT) != 0) {
                JMSPriority = in.readInt();
            }
            if ((flags & BitMap.JMS_MESSAGE_ID_BIT) != 0) {
                JMSMessageID = in.readUTF();
            }
            if ((flags & BitMap.JMS_TIMESTAMP_BIT) != 0) {
                JMSTimestamp = in.readLong();
            }
            if ((flags & BitMap.JMS_CORRELATION_ID_BIT) != 0) {
                byte type = in.readByte();
                if (type == BYTE_BIT) {
                    int size = in.readInt();
                    byte[] correlationId = new byte[size];
                    in.readFully(correlationId);
                    JMSCorrelationID = correlationId;
                } else {
                    JMSCorrelationID = in.readUTF();
                }
            }
            if ((flags & BitMap.JMS_REPLY_TO_BIT) != 0) {
                if (in instanceof ObjectInputStream) {
                    JMSReplyTo = (Destination) ((ObjectInputStream) in).readUnshared();
                } else {
                    JMSReplyTo = (Destination) in.readObject();
                }
            }
            if ((flags & BitMap.JMS_TYPE_BIT) != 0) {
                JMSType = in.readUTF();
            }
            if ((flags & BitMap.JMS_REDELIVERED_BIT) != 0) {
                JMSRedelivered = in.readBoolean();
            }
            if ((flags & BitMap.BODY_BIT) != 0) {
                if (in instanceof ObjectInputStream) {
                    Body = ((ObjectInputStream) in).readUnshared();
                } else {
                    Body = in.readObject();
                }
            }
            if ((flags & BitMap.PROPERTIES_BIT) != 0) {
                int size = in.readInt();
                Properties = new HashMap<String, Object>(size);
                for (int i = 0; i < size; i++) {
                    String name = in.readUTF();
                    Object value = readPrimitiveObject(in, name);
                    Properties.put(name, value);
                }
            }
        } catch (Exception ex) {
            if (ex instanceof EntrySerializationException)
                throw (EntrySerializationException) ex;

            String id = JMSMessageID != null ? JMSMessageID : "";
            throw new EntrySerializationException("Failed to deserialize JMS message: " + id, ex);
        }
    }


    /**
     * Writes into the output stream the primitive type according to the bit code (which is written
     * before that and indicates the primitive type), then it uses the right write method to write
     * the Primitive type itself. This way we avoid using writeObject() in case of primitive types,
     * which is very expensive.
     */
    private void writePrimitiveObject(ObjectOutput out, Object obj, String name)
            throws IOException {
        try {
            if (obj instanceof Integer) {
                out.writeByte(INTEGER_BIT);
                out.writeInt(((Integer) obj).intValue());
            } else if (obj instanceof Double) {
                out.writeByte(DOUBLE_BIT);
                out.writeDouble(((Double) obj).doubleValue());
            } else if (obj instanceof Float) {
                out.writeByte(FLOAT_BIT);
                out.writeFloat(((Float) obj).floatValue());
            } else if (obj instanceof Long) {
                out.writeByte(LONG_BIT);
                out.writeLong(((Long) obj).longValue());
            } else if (obj instanceof String &&
                    ((String) obj).length() <= 0xFFFFL) // too long written as Object instead
            {
                out.writeByte(STRING_BIT);
                out.writeUTF((String) obj);
            } else if (obj instanceof Short) {
                out.writeByte(SHORT_BIT);
                out.writeShort(((Short) obj).shortValue());
            } else if (obj instanceof Boolean) {
                out.writeByte(BOOLEAN_BIT);
                out.writeBoolean(((Boolean) obj).booleanValue());
            } else if (obj instanceof Character) {
                out.writeByte(CHAR_BIT);
                out.writeChar(((Character) obj).charValue());
            } else if (obj instanceof Byte) {
                out.writeByte(BYTE_BIT);
                out.writeByte(((Byte) obj).byteValue());
            } else {
                out.writeByte(OBJECT_BIT);
                if (out instanceof ObjectOutputStream)
                    ((ObjectOutputStream) out).writeUnshared(obj);
                else
                    out.writeObject(obj);
            }
        } catch (IOException ex) {
            throw new EntrySerializationException("Failed to serialize JMS message property: JMSMessageID="
                    + JMSMessageID + ", Property Name: " + name + ", Property Value: " + obj, ex);
        }
    }


    /**
     * Reads from input stream the primitive type according to the bit code (which indicates the
     * primitive type), then it uses the right read method to fetch the Primitive type itself.
     *
     * This way we avoid using readObject() in case of primitive types, which is very expensive.
     */
    private Object readPrimitiveObject(ObjectInput in, String name)
            throws IOException, ClassNotFoundException {
        try {
            byte bitCode = in.readByte();
            switch (bitCode) {
                case INTEGER_BIT:
                    return in.readInt();
                case DOUBLE_BIT:
                    return in.readDouble();
                case FLOAT_BIT:
                    return in.readFloat();
                case LONG_BIT:
                    return in.readLong();
                case STRING_BIT:
                    return in.readUTF();
                case SHORT_BIT:
                    //return new Short( in.readShort());
                    return in.readShort();
                case BOOLEAN_BIT:
                    return in.readBoolean();
                case CHAR_BIT:
                    return in.readChar();
                case BYTE_BIT:
                    return in.readByte();
                //object type
                default:
                    if (in instanceof ObjectInputStream) {
                        return ((ObjectInputStream) in).readUnshared();
                    } else {
                        return in.readObject();
                    }
            }
        } catch (IOException ex) {
            throw new EntrySerializationException("Failed to deserialize JMS message property: JMSMessageID="
                    + JMSMessageID + ", Property Name: " + name, ex);
        }
    }

}
