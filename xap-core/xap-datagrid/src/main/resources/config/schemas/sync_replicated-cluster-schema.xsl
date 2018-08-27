<xsl:stylesheet version="1.0" xmlns:xsl="http://www.w3.org/1999/XSL/Transform">
<xsl:output method="xml" encoding="UTF-8" omit-xml-declaration="no" indent="yes"/>
<xsl:template match="/">
	<cluster-config>
		<xsl:copy-of select="cluster-config/cluster-name"/>
		<xsl:copy-of select="cluster-config/dist-cache"/>
		<xsl:copy-of select="cluster-config/jms"/>
		<description>This topology replicates data in a synchronous manner between source and target spaces. Provides the highest level of coherency but impacts the client performance.</description>
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
             <supports-partial-update>false</supports-partial-update>
        </mirror-service>
		<cluster-members>
			<xsl:copy-of select="cluster-config/cluster-members/member"/>
		</cluster-members>
		<groups>
			<group>
				<group-name>sync_replicated_group</group-name>
					<group-members>
						<xsl:copy-of select="cluster-config/cluster-members/member"/>
					</group-members>
					<repl-policy>
						<replication-mode>sync</replication-mode>
						<permitted-operations>write, take, extend_lease, update, discard, lease_expiration, notify, evict, change</permitted-operations>
	                    <policy-type>partial-replication</policy-type>
	                    <recovery>true</recovery>
	                    <replicate-notify-templates>false</replicate-notify-templates>
	                    <trigger-notify-templates>true</trigger-notify-templates>
	                    <repl-find-timeout>5000</repl-find-timeout>
	                    <repl-find-report-interval>30000</repl-find-report-interval>
                        <repl-original-state>true</repl-original-state>
                        <repl-full-take>false</repl-full-take>
	                    <communication-mode>unicast</communication-mode>
						<redo-log-capacity>-1</redo-log-capacity>
						<redo-log-memory-capacity>-1</redo-log-memory-capacity>
						<redo-log-compaction>none</redo-log-compaction>
						<backlog-weight-policy>accumulated</backlog-weight-policy>
						<redo-log-recovery-capacity>-1</redo-log-recovery-capacity>
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
						<on-redo-log-capacity-exceeded>block-operations</on-redo-log-capacity-exceeded>
						<on-missing-packets>ignore</on-missing-packets>
						<on-conflicting-packets>ignore</on-conflicting-packets>
						<recovery-chunk-size>200</recovery-chunk-size>
						<recovery-thread-pool-size>4</recovery-thread-pool-size>
						<processing-type>global-order</processing-type>
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
                                <reliable>false</reliable>
                                <async-channel-shutdown-timeout>300000</async-channel-shutdown-timeout>
							    <repl-network-compression>true</repl-network-compression>
                        </async-replication>
	                    <sync-replication>
	                            <target-consume-timeout>10000</target-consume-timeout>
	                            <hold-txn-lock>false</hold-txn-lock>
	                            <multiple-opers-chunk-size>10000</multiple-opers-chunk-size>
	                            <throttle-when-inactive>false</throttle-when-inactive>
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
					<load-bal-policy>
  						<apply-ownership>false</apply-ownership> 
						<disable-parallel-scattering>false</disable-parallel-scattering> 
						<proxy-broadcast-threadpool-min-size>4</proxy-broadcast-threadpool-min-size> 
						<proxy-broadcast-threadpool-max-size>64</proxy-broadcast-threadpool-max-size> 
						<default>
						 	<policy-type>local-space</policy-type> 
							<broadcast-condition>never</broadcast-condition> 
						</default>
					</load-bal-policy>
					<fail-over-policy>
						<fail-back>true</fail-back>
						<fail-over-find-timeout>2000</fail-over-find-timeout>
						<default>
							<policy-type>fail-in-group</policy-type>
							
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