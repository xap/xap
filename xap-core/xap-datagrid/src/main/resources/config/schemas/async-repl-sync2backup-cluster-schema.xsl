<xsl:stylesheet version="1.0" xmlns:xsl="http://www.w3.org/1999/XSL/Transform">
    <xsl:output method="xml" encoding="UTF-8" omit-xml-declaration="no" indent="yes"/>
    <xsl:template name="create-backups">
        <xsl:param name="count" select="0"/>
        <xsl:param name="total_count" select="0"/>
        <xsl:param name="mem-name" select="mem-name"/>
        <xsl:param name="space-name" select="space-name"/>
        <xsl:if test="$count &lt; $total_count + 1">
            <member>
                <source-member>
                    <xsl:value-of select="$space-name"/>_container<xsl:value-of select="$count"/>:<xsl:value-of
                        select="$space-name"/>
                </source-member>
                <backup-member>
                    <xsl:choose>
                        <xsl:when test="$count=$total_count">
                            <xsl:value-of select="$space-name"/>_container1:<xsl:value-of select="$space-name"/>
                        </xsl:when>
                        <xsl:otherwise>
                            <xsl:value-of select="$space-name"/>_container<xsl:value-of select="$count + 1"/>:<xsl:value-of
                                select="$space-name"/>
                        </xsl:otherwise>
                    </xsl:choose>
                </backup-member>
            </member>
            <xsl:call-template name="create-backups">
                <xsl:with-param name="count" select="$count + 1"/>
                <xsl:with-param name="total_count" select="$total_count"/>
                <xsl:with-param name="mem-name" select="$mem-name"/>
                <xsl:with-param name="space-name" select="$space-name"/>
            </xsl:call-template>
        </xsl:if>
    </xsl:template>
    <xsl:template name="create-rep-matrix">
        <xsl:param name="count" select="0"/>
        <xsl:param name="total_count" select="0"/>
        <xsl:param name="mem-name" select="mem-name"/>
        <xsl:param name="space-name" select="space-name"/>
        <xsl:if test="$count &lt; $total_count + 1">
            <member>
                <member-name>
                    <xsl:value-of select="$space-name"/>_container<xsl:value-of select="$count"/>:<xsl:value-of
                        select="$space-name"/>
                </member-name>
                <repl-transmission-policy>
                    <disable-transmission>false</disable-transmission>
                    <target-member>
                        <xsl:choose>
                            <xsl:when test="$count=$total_count">
                                <xsl:value-of select="$space-name"/>_container1:<xsl:value-of select="$space-name"/>
                            </xsl:when>
                            <xsl:otherwise>
                                <xsl:value-of select="$space-name"/>_container<xsl:value-of select="$count + 1"/>:<xsl:value-of
                                    select="$space-name"/>
                            </xsl:otherwise>
                        </xsl:choose>
                    </target-member>
                    <transmission-operations>take, write, notify</transmission-operations>
                    <sync-on-commit>false</sync-on-commit>
                    <replication-mode>sync</replication-mode>
                    <communication-mode>unicast</communication-mode>
                </repl-transmission-policy>
            </member>
            <xsl:call-template name="create-rep-matrix">
                <xsl:with-param name="count" select="$count + 1"/>
                <xsl:with-param name="total_count" select="$total_count"/>
                <xsl:with-param name="mem-name" select="$mem-name"/>
                <xsl:with-param name="space-name" select="$space-name"/>
            </xsl:call-template>
        </xsl:if>
    </xsl:template>
    <xsl:template match="/">
        <xsl:variable name="total_members_count" select="count(//member)"/>
        <cluster-config>
            <xsl:copy-of select="cluster-config/cluster-name"/>
            <xsl:copy-of select="cluster-config/dist-cache"/>
            <xsl:copy-of select="cluster-config/jms"/>
            <description>Mix between the async_replicated and the sync_replicated topologies. This topology replicates
                data between cluster members in an asynchronous manner and constructs backup(s) space for each cluster
                space where replication to backup spaces is done in a synchronous manner. This topology provides good
                data coherency between primary and backup spaces with minimal performance impact when replicating data
                to other cluster members.
            </description>
            <notify-recovery>true</notify-recovery>
            <cache-loader>
                <external-data-source>${com.gs.cluster.cache-loader.external-data-source}</external-data-source>
                <central-data-source>${com.gs.cluster.cache-loader.central-data-source}</central-data-source>
            </cache-loader>
            <mirror-service>
                <enabled>false</enabled>
                <url>jini://*/mirror-service_container/mirror-service</url>
                <bulk-size>100</bulk-size>
                <interval-millis>2000</interval-millis>
                <interval-opers>100</interval-opers>
                <on-redo-log-capacity-exceeded>block-operations</on-redo-log-capacity-exceeded>
            </mirror-service>
            <cluster-members>
                <xsl:copy-of select="cluster-config/cluster-members/member"/>
            </cluster-members>
            <groups>
                <group>
                    <group-name>async_replicated_group</group-name>
                    <group-members>
                        <xsl:variable name="mem-name" select="cluster-config/cluster-members/member/member-name"/>
                        <xsl:variable name="mem-url" select="cluster-config/cluster-members/member/member-url"/>
                        <xsl:variable name="space-name" select="substring-after($mem-name,':')"/>
                        <xsl:call-template name="create-rep-matrix">
                            <xsl:with-param name="count" select="1"/>
                            <xsl:with-param name="total_count" select="$total_members_count"/>
                            <xsl:with-param name="mem-name" select="$mem-name"/>
                            <xsl:with-param name="space-name" select="$space-name"/>
                        </xsl:call-template>
                    </group-members>
                    <repl-policy>
                        <replication-mode>async</replication-mode>
                        <policy-type>partial-replication</policy-type>
                        <permitted-operations>write, take, extend_lease, update, discard, lease_expiration, notify,
                            evict, change
                        </permitted-operations>
                        <recovery>true</recovery>
                        <replicate-notify-templates>true</replicate-notify-templates>
                        <trigger-notify-templates>false</trigger-notify-templates>
                        <repl-find-timeout>5000</repl-find-timeout>
                        <repl-find-report-interval>30000</repl-find-report-interval>
                        <repl-original-state>true</repl-original-state>
                        <repl-full-take>false</repl-full-take>
                        <communication-mode>unicast</communication-mode>
                        <redo-log-capacity>-1</redo-log-capacity>
                        <redo-log-recovery-capacity>-1</redo-log-recovery-capacity>
                        <redo-log-memory-capacity>-1</redo-log-memory-capacity>
                        <redol-log-compaction>none</redol-log-compaction>
                        <backlog-weight-policy>accumulated</backlog-weight-policy>
                        <on-redo-log-capacity-exceeded>block-operations</on-redo-log-capacity-exceeded>
                        <on-missing-packets>recover</on-missing-packets>
                        <recovery-chunk-size>200</recovery-chunk-size>
                        <recovery-thread-pool-size>4</recovery-thread-pool-size>
                        <async-replication>
                            <sync-on-commit>false</sync-on-commit>
                            <sync-on-commit-timeout>300000</sync-on-commit-timeout>
                            <repl-chunk-size>500</repl-chunk-size>
                            <repl-interval-millis>3000</repl-interval-millis>
                            <repl-interval-opers>500</repl-interval-opers>
                            <reliable>true</reliable>
                            <async-channel-shutdown-timeout>300000</async-channel-shutdown-timeout>
                            <repl-network-compression>true</repl-network-compression>
                        </async-replication>
                        <sync-replication>
                            <todo-queue-timeout>1500</todo-queue-timeout>
                            <hold-txn-lock>false</hold-txn-lock>
                            <multiple-opers-chunk-size>10000</multiple-opers-chunk-size>
                            <unicast>
                                <min-work-threads>4</min-work-threads>
                                <max-work-threads>16</max-work-threads>
                            </unicast>
                            <multicast>
                                <adaptive>true</adaptive>
                                <ip-group>224.0.0.1</ip-group>
                                <port>28672</port>
                                <ttl>4</ttl>
                                <min-work-threads>4</min-work-threads>
                                <max-work-threads>16</max-work-threads>
                            </multicast>
                        </sync-replication>
                    </repl-policy>
                    <load-bal-policy>
                        <apply-ownership>false</apply-ownership>
                        <disable-parallel-scattering>false</disable-parallel-scattering>
                        <proxy-broadcast-threadpool-min-size>4</proxy-broadcast-threadpool-min-size>
                        <proxy-broadcast-threadpool-max-size>64</proxy-broadcast-threadpool-max-size>
                        <notify>
                            <policy-type>local-space</policy-type>
                            <broadcast-condition>never</broadcast-condition>
                        </notify>
                        <default>
                            <policy-type>hash-based</policy-type>
                            <broadcast-condition>routing-index-is-null</broadcast-condition>
                        </default>
                    </load-bal-policy>
                    <fail-over-policy>
                        <fail-back>true</fail-back>
                        <fail-over-find-timeout>2000</fail-over-find-timeout>
                        <default>
                            <policy-type>fail-to-backup</policy-type>

                            <backup-members>
                                <xsl:variable name="mem-name"
                                              select="cluster-config/cluster-members/member/member-name"/>
                                <xsl:variable name="mem-url" select="cluster-config/cluster-members/member/member-url"/>
                                <xsl:variable name="space-name" select="substring-after($mem-name,':')"/>
                                <xsl:call-template name="create-backups">
                                    <xsl:with-param name="count" select="1"/>
                                    <xsl:with-param name="total_count" select="$total_members_count"/>
                                    <xsl:with-param name="mem-name" select="$mem-name"/>
                                    <xsl:with-param name="space-name" select="$space-name"/>
                                </xsl:call-template>
                            </backup-members>
                        </default>
                        <active-election>
                            <connection-retries>60</connection-retries>
                            <yield-time>1000</yield-time>
                            <fault-detector>
                                <invocation-delay>1000</invocation-delay>
                                <retry-count>3</retry-count>
                                <retry-timeout>100</retry-timeout>
                            </fault-detector>
                        </active-election>
                    </fail-over-policy>
                </group>
            </groups>
        </cluster-config>
    </xsl:template>
</xsl:stylesheet>
