package com.mesosphere.dcos.cassandra.common.offer;

import com.mesosphere.sdk.offer.OfferRequirement;
import org.apache.mesos.Protos;

public interface CassandraOfferRequirementProvider {

    OfferRequirement getNewOfferRequirement(String type, Protos.TaskInfo taskInfo);

    OfferRequirement getReplacementOfferRequirement(String type, Protos.TaskInfo taskInfo);

    OfferRequirement getUpdateOfferRequirement(String type, Protos.TaskInfo info);
}
