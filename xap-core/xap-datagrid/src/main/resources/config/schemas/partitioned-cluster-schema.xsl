<xsl:stylesheet version="1.0" xmlns:xsl="http://www.w3.org/1999/XSL/Transform">
	<xsl:output method="xml" encoding="UTF-8" omit-xml-declaration="no" indent="yes"/>
	<xsl:template name="create-backups">
		<xsl:param name="count" select="0"/>
		<xsl:param name="mem-name" select="mem-name"/>
		<xsl:param name="space-name" select="space-name"/>
		<xsl:param name="with-urls" select="1"/>
		<xsl:param name="source" select="1"/>
		<xsl:param name="only" select="1"/>
		<xsl:variable name="groups-value" select="@jini-groups" />
		<xsl:if test="$count > 0">
			<xsl:choose>
				<xsl:when test="$source=1">
					<backup-member>
						<xsl:value-of select="@backup-container"/>_<xsl:value-of select="$count"/>:<xsl:value-of select="$space-name"/>
					</backup-member>
				</xsl:when>
				<xsl:when test="$only=1">
					<backup-member-only>
						<xsl:value-of select="@backup-container"/>_<xsl:value-of select="$count"/>:<xsl:value-of select="$space-name"/>
					</backup-member-only>
				</xsl:when>
				<xsl:otherwise>
					<member>
						<member-name>
							<xsl:value-of select="@backup-container"/>_<xsl:value-of select="$count"/>:<xsl:value-of select="$space-name"/>
						</member-name>
						<xsl:if test="$with-urls='1'">
							<member-url><xsl:value-of select="@member-prefix"/><xsl:value-of select="@backup-container"/>_<xsl:value-of select="$count"/>/<xsl:value-of select="$space-name"/><xsl:value-of select="$groups-value"/>
							</member-url>
						</xsl:if>
					</member>
				</xsl:otherwise>
			</xsl:choose>
			<xsl:call-template name="create-backups">
				<xsl:with-param name="count" select="$count - 1"/>
				<xsl:with-param name="mem-name" select="$mem-name"/>
				<xsl:with-param name="space-name" select="$space-name"/>
				<xsl:with-param name="with-urls" select="$with-urls"/>
				<xsl:with-param name="source" select="$source"/>
				<xsl:with-param name="only" select="$only"/>
			</xsl:call-template>
		</xsl:if>
	</xsl:template>
	<xsl:template match="/">
		<cluster-config>
			<xsl:copy-of select="cluster-config/cluster-name"/>
			<xsl:copy-of select="cluster-config/dist-cache"/>
			<xsl:copy-of select="cluster-config/jms"/>
			<description>This topology constructs backup(s) for each partitioned space. Replication from each partition to its backup space is done in a synchronous manner.</description>
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
	            <redo-log-capacity>1000000</redo-log-capacity>
	            <supports-partial-update>false</supports-partial-update>
	            <change-support>none</change-support>	            
	        </mirror-service>
			<cluster-members>
				<xsl:for-each select="cluster-config/cluster-members/member">
					<xsl:variable name="mem-name" select="member-name"/>
					<xsl:variable name="mem-url" select="member-url"/>
					<xsl:variable name="space-name" select="substring-after($mem-name,':')"/>
					<member>
						<member-name>
							<xsl:value-of select="$mem-name"/>
						</member-name>
						<member-url>
							<xsl:value-of select="$mem-url"/>
						</member-url>
					</member>
					<xsl:call-template name="create-backups">
						<xsl:with-param name="count" select="@number-backups"/>
						<xsl:with-param name="mem-name" select="$mem-name"/>
						<xsl:with-param name="space-name" select="$space-name"/>
						<xsl:with-param name="with-urls" select="1"/>
						<xsl:with-param name="source" select="0"/>
						<xsl:with-param name="only" select="0"/>
					</xsl:call-template>
				</xsl:for-each>
			</cluster-members>
			<groups>
				<group>
					<group-name>LB_FO_Backup</group-name>
					<group-members>
						<xsl:for-each select="cluster-config/cluster-members/member">
							<xsl:variable name="mem-name" select="member-name"/>
							<xsl:variable name="mem-url" select="member-url"/>
							<xsl:variable name="space-name" select="substring-after($mem-name,':')"/>
							<member>
								<member-name>
									<xsl:value-of select="$mem-name"/>
								</member-name>
							</member>
							<xsl:call-template name="create-backups">
								<xsl:with-param name="count" select="@number-backups"/>
								<xsl:with-param name="mem-name" select="$mem-name"/>
								<xsl:with-param name="space-name" select="$space-name"/>
								<xsl:with-param name="with-urls" select="0"/>
								<xsl:with-param name="source" select="0"/>
								<xsl:with-param name="only" select="0"/>
							</xsl:call-template>
						</xsl:for-each>
					</group-members>
					<load-bal-policy>
						<apply-ownership>false</apply-ownership>						
						<disable-parallel-scattering>false</disable-parallel-scattering>						
						<proxy-broadcast-threadpool-min-size>4</proxy-broadcast-threadpool-min-size>
						<proxy-broadcast-threadpool-max-size>64</proxy-broadcast-threadpool-max-size>						
						<notify>
							<policy-type>hash-based</policy-type>
							<broadcast-condition>routing-index-is-null</broadcast-condition>
						</notify>
						<default>
							<policy-type>hash-based</policy-type>
							<broadcast-condition>routing-index-is-null</broadcast-condition>
						</default>
					</load-bal-policy>
					<fail-over-policy>
						<fail-back>false</fail-back>
						<fail-over-find-timeout>2000</fail-over-find-timeout>				
						<default>
							<policy-type>fail-to-backup</policy-type>
							
							<backup-members>
								<xsl:for-each select="cluster-config/cluster-members/member">
									<xsl:variable name="mem-name" select="member-name"/>
									<xsl:variable name="mem-url" select="member-url"/>
									<xsl:variable name="space-name" select="substring-after($mem-name,':')"/>
									<member>
										<source-member>
											<xsl:value-of select="$mem-name"/>
										</source-member>
										<xsl:call-template name="create-backups">
											<xsl:with-param name="count" select="@number-backups"/>
											<xsl:with-param name="mem-name" select="$mem-name"/>
											<xsl:with-param name="space-name" select="$space-name"/>
											<xsl:with-param name="with-urls" select="0"/>
											<xsl:with-param name="source" select="1"/>
											<xsl:with-param name="only" select="0"/>
										</xsl:call-template>
									</member>
								</xsl:for-each>
							</backup-members>
							<backup-members-only>
								<xsl:for-each select="cluster-config/cluster-members/member">
									<xsl:variable name="mem-name" select="member-name"/>
									<xsl:variable name="mem-url" select="member-url"/>
									<xsl:variable name="space-name" select="substring-after($mem-name,':')"/>
									<xsl:call-template name="create-backups">
										<xsl:with-param name="count" select="@number-backups"/>
										<xsl:with-param name="mem-name" select="$mem-name"/>
										<xsl:with-param name="space-name" select="$space-name"/>
										<xsl:with-param name="with-urls" select="0"/>
										<xsl:with-param name="source" select="0"/>
										<xsl:with-param name="only" select="1"/>
									</xsl:call-template>
								</xsl:for-each>
							</backup-members-only>
						</default>
						<active-election>
							<connection-retries>60</connection-retries>
							<yield-time>1000</yield-time>
							<resolution-timeout>10000</resolution-timeout>
						    <fault-detector>
						        <invocation-delay>1000</invocation-delay>
						        <retry-count>3</retry-count>
						        <retry-timeout>100</retry-timeout>
						   </fault-detector>   		
						</active-election>							
					</fail-over-policy>
				</group>
				<xsl:for-each select="cluster-config/cluster-members/member">
					<xsl:variable name="mem-name" select="member-name"/>
					<xsl:variable name="mem-url" select="member-url"/>
					<xsl:variable name="space-name" select="substring-after($mem-name,':')"/>
					<group>
						<group-name>replGroup<xsl:value-of select="$space-name"/>_<xsl:value-of select="position()-1"/>
						</group-name>
						<group-members>
							<member>
								<member-name>
									<xsl:value-of select="$mem-name"/>
								</member-name>
							</member>
							<xsl:call-template name="create-backups">
								<xsl:with-param name="count" select="@number-backups"/>
								<xsl:with-param name="mem-name" select="$mem-name"/>
								<xsl:with-param name="space-name" select="$space-name"/>
								<xsl:with-param name="with-urls" select="0"/>
								<xsl:with-param name="source" select="0"/>
								<xsl:with-param name="only" select="0"/>
							</xsl:call-template>
						</group-members>
						<repl-policy>
							<replication-mode>sync</replication-mode>
							<permitted-operations>write, take, extend_lease, update, discard, lease_expiration, notify, evict, change</permitted-operations>
							<policy-type>partial-replication</policy-type>
							<recovery>true</recovery>
							<replicate-notify-templates>true</replicate-notify-templates>
							<trigger-notify-templates>false</trigger-notify-templates>
							<replicate-lease-expirations>true</replicate-lease-expirations>
							<repl-find-timeout>5000</repl-find-timeout>
							<repl-find-report-interval>30000</repl-find-report-interval>
	                        <repl-original-state>true</repl-original-state>
	                        <repl-full-take>false</repl-full-take>
	                        <repl-one-phase-commit>false</repl-one-phase-commit>	                        
							<communication-mode>unicast</communication-mode>
							<redo-log-capacity>150000</redo-log-capacity>
							<redo-log-recovery-capacity>5000000</redo-log-recovery-capacity>
							<redo-log-memory-capacity>150000</redo-log-memory-capacity>
							<redo-log-compaction>none</redo-log-compaction>
							<backlog-weight-policy>accumulated</backlog-weight-policy>
							<redo-log-local-view-capacity>150000</redo-log-local-view-capacity>
							<redo-log-local-view-recovery-capacity>1000000</redo-log-local-view-recovery-capacity>
							<local-view-max-disconnection-time>300000</local-view-max-disconnection-time>
							<redo-log-durable-notification-capacity>150000</redo-log-durable-notification-capacity>
							<durable-notification-max-disconnection-time>300000</durable-notification-max-disconnection-time>
							<swap-redo-log>
								<!--This are low level properties that has to do with the swap redo log mechanism, this should not be changed normally
								    The swap redo log is only used when the redo log memory capacity is limited (not -1)-->
								<flush-buffer-packet-count>500</flush-buffer-packet-count>
								<fetch-buffer-packet-count>500</fetch-buffer-packet-count>
								<segment-size>10485760</segment-size>
								<max-scan-length>51200</max-scan-length>
								<max-open-cursors>10</max-open-cursors>
								<writer-buffer-size>262144</writer-buffer-size>
							</swap-redo-log>
							<on-redo-log-capacity-exceeded>drop-oldest</on-redo-log-capacity-exceeded>
							<on-missing-packets>recover</on-missing-packets>
							<!-- <on-conflicting-packets>ignore</on-conflicting-packets> -->
							<recovery-chunk-size>200</recovery-chunk-size>
							<recovery-thread-pool-size>4</recovery-thread-pool-size>
                            <connection-monitor-thread-pool-size>4</connection-monitor-thread-pool-size>
                            <processing-type>multi-source</processing-type>
							<multi-bucket-processing>
								<bucket-count>1024</bucket-count>
								<batch-parallel-threshold>50</batch-parallel-threshold>
								<batch-parallel-factor>default</batch-parallel-factor>
							</multi-bucket-processing>
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
								<consistency-level>any</consistency-level>
							    <target-consume-timeout>10000</target-consume-timeout>
							    <hold-txn-lock>false</hold-txn-lock>
							    <multiple-opers-chunk-size>10000</multiple-opers-chunk-size>
							    <throttle-when-inactive>true</throttle-when-inactive>
							    <max-throttle-tp-when-inactive>50000</max-throttle-tp-when-inactive>
							    <min-throttle-tp-when-active>1000</min-throttle-tp-when-active>
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
					</group>
				</xsl:for-each>
			</groups>
		</cluster-config>
	</xsl:template>
</xsl:stylesheet>
