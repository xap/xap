<#--
// Licensed to the Apache Software Foundation (ASF) under one or more
// contributor license agreements.  See the NOTICE file distributed with
// this work for additional information regarding copyright ownership.
// The ASF licenses this file to you under the Apache License, Version 2.0
// (the "License"); you may not use this file except in compliance with
// the License.  You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
-->

SqlShowOption SqlShowOption() :
{
  final Span s;
  final SqlIdentifier name;
}
{
  <SHOW> { s = span(); }
  name = CompoundIdentifier()
  {
      return new SqlShowOption(s.end(name), name);
  }
}

SqlSetOption SqlSetOptionAlt() :
{
    final Span s = span();
    final SqlIdentifier name;
    final SqlNode val;
}
{
    (
        LOOKAHEAD(2)
        <SET> <SESSION> <CHARACTERISTICS> <AS> <TRANSACTION> <ISOLATION> <LEVEL>
        {
            name = new SqlIdentifier("TRANSACTION_ISOLATION", getPos());
        }
        (
            LOOKAHEAD(2)
            <READ> <COMMITTED> {
                val = SqlLiteral.createCharString("READ_COMMITTED", getPos());
            }
        |
            <READ> <UNCOMMITTED> {
                val = SqlLiteral.createCharString("READ_UNCOMMITTED", getPos());
            }
        |
            <REPEATABLE> <READ> {
                val = SqlLiteral.createCharString("REPEATABLE_READ", getPos());
            }
        |
            <SERIALIZABLE> {
                val = SqlLiteral.createCharString("SERIALIZABLE", getPos());
            }
        )
        {
            return new SqlSetOption(getPos(), null, name, val);
        }
    |
        <SET> {
            s.add(this);
        }
        name = CompoundIdentifier()
        (<EQ>|<TO>)
        (
            val = Literal()
        |
            val = SimpleIdentifier()
        |
            <ON> {
                // OFF is handled by SimpleIdentifier, ON handled here.
                val = new SqlIdentifier(token.image.toUpperCase(Locale.ROOT),
                    getPos());
            }
        )
        {
            return new SqlSetOption(s.end(val), null, name, val);
        }
    )
}
