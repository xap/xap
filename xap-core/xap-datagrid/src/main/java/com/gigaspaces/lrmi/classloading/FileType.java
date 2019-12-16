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
package com.gigaspaces.lrmi.classloading;
/**
 * Enum of type of file (class/resource) to be loaded by the class provider.
 *
 * @see ClassDefinitionRequest
 * @see ClassDefinitionResponse
 *
 * @author alon shoham
 * @since 15.0
 */
public enum FileType {CLASS, RESOURCE}
