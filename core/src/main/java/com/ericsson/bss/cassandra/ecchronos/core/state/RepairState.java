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
package com.ericsson.bss.cassandra.ecchronos.core.state;

/**
 * Interface used by TableRepairJob to update and get a snapshot of the current repair state of a table.
 *
 * @see RepairStateSnapshot
 */
public interface RepairState
{
    /**
     * Update the repair state for the table in the specified node.
     */
    void update();

    /**
     * Get an immutable copy of the current repair state.
     *
     * @return The immutable copy.
     */
    RepairStateSnapshot getSnapshot();
}