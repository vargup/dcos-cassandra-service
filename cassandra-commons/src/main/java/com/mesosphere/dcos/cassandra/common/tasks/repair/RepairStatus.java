/*
 * Copyright 2016 Mesosphere
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.mesosphere.dcos.cassandra.common.tasks.repair;


import com.mesosphere.dcos.cassandra.common.tasks.CassandraTaskStatus;
import org.apache.mesos.Protos;

/**
 * RepairStatus extends CassandraTaskStatus to implement the status Object for
 * RepairTask.
 */
public class RepairStatus extends CassandraTaskStatus {

    public static RepairStatus create(final Protos.TaskStatus status) {
        return new RepairStatus(status);
    }

    protected RepairStatus(final Protos.TaskStatus status) {
        super(status);
    }

}
