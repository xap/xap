@echo off
rem #*******************************************************************************************************
rem # Use this script to override default GigaSpaces settings using environment variables.
rem # For more information see https://docs.gigaspaces.com/15.8/started/common-environment-variables.html
rem #
rem # Common variables:
rem # -----------------
rem # JAVA_HOME          - The directory in which Java is installed
rem # GS_OPTIONS_EXT     - Extra JVM options and system properties to append to java command line
rem # GS_CLASSPATH_EXT   - Extra classpath to append to the predefined classpath
rem # GS_NIC_ADDRESS     - The network interface card / host name/address to use (defaults to hostname)
rem # GS_LOOKUP_GROUPS   - Lookup groups used for multicast discovery (defaults to version identifier)
rem # GS_LOOKUP_LOCATORS - Lookup locators used for unicast discovery
rem #*******************************************************************************************************

rem set JAVA_HOME=
rem set GS_OPTIONS_EXT=
rem set GS_CLASSPATH_EXT=
rem set GS_NIC_ADDRESS=
rem set GS_LOOKUP_GROUPS=
rem set GS_LOOKUP_LOCATORS=