# GIGASPACES XAP SOFTWARE - OPEN SOURCE AND THIRD PARTY COMPONENTS

GigaSpaces XAP dependencies can be categorized as follows:

* Mandatory Core Dependencies - Dependencies which are essential for XAP core, and cannot be removed without harming XAP.
* Optional Core Dependencies - Dependencies which enhance XAP core, but can be removed if needed.
* Optional Extensions - Dependencies which are required for XAP various extensions. Each such dependency is listed in the content of the relevant extension. If the extension is not required, the dependency can be removed.

## Mandatory Core Dependencies

Located at GS_HOME/lib/required

### Spring (under the Apache License, Version 2.0)

    http://www.springframework.org/about
    http://www.apache.org/licenses/LICENSE-2.0.html

Copyright 2010, www.springsource.org

Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the License. You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0.

Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" basis, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language governing permissions and limitations under the License.

###	Commons Logging (under the Apache License, Version 2.0)

    http://jakarta.apache.org/commons/logging/
    http://www.apache.org/licenses/LICENSE-2.0.html

Copyright (c) 2010 The Apache Software Foundation

Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the License. You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0.

Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" basis, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language governing permissions and limitations under the License.

###	Jini (under the Apache License, Version 2.0)

    http://www.sun.com/software/jini/
    http://www.apache.org/licenses/LICENSE-2.0.html

Copyright 2005 Sun Microsystems, Inc.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

 http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.

###	Rio (under the Apache License, Version 2.0)

    http://community.java.net/jini/
    http://www.apache.org/licenses/LICENSE-2.0.html

Copyright 2005 Sun Microsystems, Inc.

Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the License. You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0.

Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" basis, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language governing permissions and limitations under the License.

## Optional - Core enhancements dependencies

###	ASM (under "BSD-style" license)

Provides reflections enhancements, essentially making XAP run faster. Located at GS_HOME/lib/required/xap-asm.jar

    http://asm.objectweb.org/doc/tutorial.html
    ASM - http://asm.objectweb.org/doc/tutorial.html

Copyright (c) 2000-2005 INRIA, France Telecom
All rights reserved.

Redistribution and use in source and binary forms, with or without modification, are permitted provided that the following conditions are met:

1. Redistributions of source code must retain the above copyright notice, this list of conditions and the following disclaimer.

2. Redistributions in binary form must reproduce the above copyright notice, this list of conditions and the following disclaimer in the documentation and/or other materials provided with the distribution.

3. Neither the name of the copyright holders nor the names of its contributors may be used to endorse or promote products derived from this software without specific prior written permission.

THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT OWNER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.

###	Trove (under GNU Lesser General Public License)

Provides alternative collections framework which is more efficient is some scenarios (mostly for primitives), essentially making XAP run faster. Located at GS_HOME/lib/required/xap-trove.jar

    http://trove4j.sourceforge.net/
    http://trove4j.sourceforge.net/html/license.html

Copyright (c) 2001, Eric D. Friedman All Rights Reserved.

This library is free software; you can redistribute it and/or modify it under the terms of the GNU Lesser General Public License as published by the Free Software Foundation; either version 2.1 of the License, or (at your option) any later version. GigaSpaces is following the terms and conditions of LGPL v.3

The Trove software was modified by GigaSpaces in 2011 ("Modified Software").

Such Modified Software is distributed pursuant to the GNU Lesser General Public License, version 3. At all time the source code of the Modified Software is made available at http://www.gigaspaces.com/tempfiles/opensource/8.0/trove/gigaspaces-trove-src.zip.

Notwithstanding any other terms herein, you may modify the Modified Software for your own use and reverse engineer it for debugging such Modified Software.

GNU LGPL information.http://www.gnu.org/licenses/lgpl-3.0.txt

http://www.gnu.org/licenses/gpl.txt

This library is free software; you can redistribute it and/or modify it under the terms of the GNU Lesser General Public License as published by the Free Software Foundation; either version 2.1 of the License, or (at your option) any later version.

This library is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU General Public License for more details. You should have received a copy of the GNU Lesser General Public License along with this program; if not, write to the Free Software Foundation, Inc., 59 Temple Place - Suite 330, Boston, MA 02111-1307, USA.

You may also contact GigaSpaces for more information at this address: 317 Madison Ave, Suite 823, New York, NY 10017, USA and via email at info@gigaspaces.com.


Two classes (HashFunctions and PrimeFinder) included in Trove are licensed under the following terms:
Copyright (c) 1999 CERN - European Organization for Nuclear Research. Permission to use, copy, modify, distribute and sell this software and its documentation for any purpose is hereby granted without fee, provided that the above copyright notice appear in all copies and that both that copyright notice and this permission notice appear in supporting documentation. CERN makes no representations about the suitability of this software for any purpose. It is provided "as is" without expressed or implied warranty.

###	Commons Lang (under the Apache License, Version 2.0)

    http://commons.apache.org/lang/license.html
    http://commons.apache.org/lang/

Copyright (c) 2001-2011 The Apache Software Foundation. All Rights Reserved

Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the License. You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0.

Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" basis, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language governing permissions and limitations under the License.

## Optional - JMS integration dependencies

xap-jms extension provides JMS integration. Located at GS_HOME/lib/optional/jms

###	JMS (under Sun Public License Version 1.0)

    http://java.sun.com/products/jms/
    http://www.opensource.org/licenses/sunpublic.php

The source code of the JMS software is available under the terms Sun Public License Version 1.0 http://www.opensource.org/licenses/sunpublic.php.

Notice: the GigaSpaces license agreement does not attempt to limit or alter the recipient's
rights in the JMS's source code version from the rights set forth in the Sun Public License Version 1.0. Any terms in the Sun Public License Version 1.0 which differ from the Sun Public License Version 1.0 are offered by GigaSpaces alone, not by the initial
developer or any contributor of the JMS software.

## Optional - JDBC integration dependencies

Located at GS_HOME/lib/optional/jdbc

###	HyperSonic SQL (based under BSD license)

    http://www.hsqldb.org
    http://www.hsqldb.org/web/hsqlLicense.html

For work developed by the HSQL Development Group:
Copyright (c) 2001-2010, The HSQL Development Group
All rights reserved.

Redistribution and use in source and binary forms, with or without modification, are permitted provided that the following conditions are met:

Redistributions of source code must retain the above copyright notice, this list of conditions and the following disclaimer.

Redistributions in binary form must reproduce the above copyright notice, this list of conditions and the following disclaimer in the documentation and/or other materials provided with the distribution.

Neither the name of the HSQL Development Group nor the names of its contributors may be used to endorse or promote products derived from this software without specific prior written permission.

THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL HSQL EVELOPMENT GROUP, HSQLDB.ORG, OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL,  EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.

For work originally developed by the Hypersonic SQL Group:

Copyright (c) 1995-2000 by the Hypersonic SQL Group.
All rights reserved.
Redistribution and use in source and binary forms, with or without modification, are permitted provided that the following conditions are met:

Redistributions of source code must retain the above copyright notice, this list of conditions and the following disclaimer.

Redistributions in binary form must reproduce the above copyright notice, this list of conditions and the following disclaimer in the documentation and/or other materials provided with the distribution.

Neither the name of the Hypersonic SQL Group nor the names of its contributors may be used to endorse or promote products derived from this software without specific prior written permission.

THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE HYPERSONIC SQL GROUP,  OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL,  EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO,  PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES;
LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
This software consists of voluntary contributions made by many individuals on behalf of the
Hypersonic SQL Group.

###	H2 (dual licensed and available under a modified version of the MPL 1.1 (Mozilla Public License) or under the (unmodified) EPL 1.0 (Eclipse Public License).)

    http://www.h2database.com/
    http://www.h2database.com/html/license.html

THE H2 PROGRAM IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, EITHER EXPRESS OR IMPLIED INCLUDING, WITHOUT LIMITATION, ANY WARRANTIES OR CONDITIONS OF TITLE, NON-INFRINGEMENT, MERCHANTABILITY OR FITNESS FOR A PARTICULAR PURPOSE. Each recipient of the H2 program is solely responsible for determining the appropriateness of using and distributing the H2 program and assumes all risks associated with its exercise of rights under the Eclipse Public license agreement which is available at http://eclipse.org/legal/epl-v10.html, including but not limited to the risks and costs of program errors, compliance with applicable laws, damage to or loss of data, programs or equipment, and unavailability or interruption of operations. Any provision which is differs from the Eclipse Public License is offered by the contributor alone and not by any other party.
EXCEPT AS EXPRESSLY SET FORTH IN THE ECLIPSE PUBLIC LICENSE AGREEMENT, NEITHER RECIPIENT NOR ANY CONTRIBUTORS SHALL HAVE ANY LIABILITY FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING WITHOUT LIMITATION LOST PROFITS), HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OR DISTRIBUTION OF THE ASPECTJWEAVER PROGRAM OR THE EXERCISE OF ANY RIGHTS GRANTED HEREUNDER, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGES.
The source code for the H2 program is available from:
http://code.google.com/p/h2database/source/checkout

## Optional - Jetty integration dependencies

XAP provides integration with Jetty, mainly in the form of a web processing unit. Located at:

* GS_HOME/lib/optional/jetty
* GS_HOME/lib/optional/jetty-9

###	Jetty (under dual licensed under the Apache License 2.0 and Eclipse Public License 1.0)

    http://www.mortbay.org/jetty/
    http://www.eclipse.org/jetty/licenses.php

Jetty Web Container
 Copyright 1995-2009 Mort Bay Consulting Pty Ltd

Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the License. You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0.

Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" basis, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language governing permissions and limitations under the License.

The Jetty Web Container is Copyright Mort Bay Consulting Pty Ltd unless otherwise noted. It is dual licensed under the apache 2.0 license and eclipse 1.0 license. Jetty may be distributed under
either license.

The javax.servlet package used was sourced from the Apache Software Foundation and is distributed under the apache 2.0 license.


The UnixCrypt.java code implements the one way cryptography used by Unix systems for simple password protection.  Copyright 1996 Aki Yoshida, modified April 2001 by Iris Van den Broeke, Daniel Deville. Permission to use, copy, modify and distribute UnixCrypt for non-commercial or commercial purposes and without fee is granted provided that the copyright notice appears in all copies.

## Optional - Spatial API dependencies

XAP provides geospatial capabilities via integration with Lucene. Located at GS_HOME/lib/optional/spatial

###	Apache Lucene

    http://www.apache.org/licenses/LICENSE-2.0

Project is licensed under Apache License 2.0 (http://www.apache.org/licenses/LICENSE-2.0.txt).

you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.

###	JTS (under GNU Lesser General Public License)

    http://www.vividsolutions.com/jts/JTSHome.htm
    http://www.gnu.org/copyleft/lesser.html

JTS library is published under the OSI approved GNU LESSER GENERAL PUBLIC LICENSE.

The license for the JTS GigaSpaces uses is the GNU LGPL, Version 3, which is attached hereto.

GNU LGPL information. http://www.gnu.org/licenses/lgpl-3.0.txt

This library is free software; you can redistribute it and/or  modify it under the terms of the GNU Lesser General Public License as published by the Free Software Foundation; either  version 2.1 of the License, or (at your option) any later version.

http://www.gnu.org/licenses/lgpl.txt

This library is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU Lesser General Public License for more details.

You should have received a copy of the GNU Lesser General Public License along with this library; if not, write to the Free Software Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301, USA.

You may also contact GigaSpaces for more information at this address: 317 Madison Ave, Suite 823, New York, NY 10017, USA and via email at info@gigaspaces.com.
