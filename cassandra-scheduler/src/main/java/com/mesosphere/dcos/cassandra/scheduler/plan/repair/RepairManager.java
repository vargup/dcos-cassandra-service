package com.mesosphere.dcos.cassandra.scheduler.plan.repair;

import com.google.inject.Inject;
import com.mesosphere.dcos.cassandra.common.offer.ClusterTaskOfferRequirementProvider;
import com.mesosphere.dcos.cassandra.common.persistence.PersistenceException;
import com.mesosphere.dcos.cassandra.common.tasks.CassandraState;
import com.mesosphere.dcos.cassandra.common.tasks.ClusterTaskManager;
import com.mesosphere.dcos.cassandra.common.tasks.repair.RepairContext;
import com.mesosphere.dcos.cassandra.scheduler.resources.RepairRequest;
import com.mesosphere.sdk.scheduler.plan.DefaultPhase;
import com.mesosphere.sdk.scheduler.plan.Phase;
import com.mesosphere.sdk.scheduler.plan.Step;
import com.mesosphere.sdk.scheduler.plan.strategy.SerialStrategy;
import com.mesosphere.sdk.state.StateStore;

import java.util.*;
import java.util.stream.Collectors;

public class RepairManager extends ClusterTaskManager<RepairRequest, RepairContext> {
    static final String REPAIR_KEY = "repair";

    private final CassandraState cassandraState;
    private final ClusterTaskOfferRequirementProvider provider;

    @Inject
    public RepairManager(
            CassandraState cassandraState,
            ClusterTaskOfferRequirementProvider provider,
            StateStore stateStore) {
        super(stateStore, REPAIR_KEY, RepairContext.class);
        this.provider = provider;
        this.cassandraState = cassandraState;
        restore();
    }

    @Override
    protected RepairContext toContext(RepairRequest request) {
        return request.toContext(cassandraState);
    }

    @Override
    protected List<Phase> createPhases(RepairContext context) {
        final Set<String> nodes = new HashSet<>(context.getNodes());
        final List<String> daemons = new ArrayList<>(cassandraState.getDaemons().keySet());
        Collections.sort(daemons);
        List<Step> steps = daemons.stream()
                .filter(daemon -> nodes.contains(daemon))
                .map(daemon -> new RepairStep(daemon, cassandraState, provider, context))
                .collect(Collectors.toList());
        return Arrays.asList(new DefaultPhase("Repair", steps, new SerialStrategy<>(), Collections.emptyList()));
    }

    @Override
    protected void clearTasks() throws PersistenceException {
        cassandraState.remove(cassandraState.getRepairTasks().keySet());
    }
}