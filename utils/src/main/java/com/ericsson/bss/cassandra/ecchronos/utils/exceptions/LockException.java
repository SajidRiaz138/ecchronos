/*
 * Copyright 2024 Telefonaktiebolaget LM Ericsson
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.ericsson.bss.cassandra.ecchronos.utils.exceptions;

/**
 * Exception thrown when a lock factory is unable to get a lock.
 */
public class LockException extends Exception
{
    private static final long serialVersionUID = 1699712279389641954L;

    public LockException(final String message)
    {
        super(message);
    }

    public LockException(final String message, final Throwable t)
    {
        super(message, t);
    }

    public LockException(final Throwable t)
    {
        super(t);
    }
}