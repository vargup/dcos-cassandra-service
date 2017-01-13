package com.mesosphere.dcos.cassandra.scheduler.plan.upgradesstable;

import com.google.inject.Inject;
import com.mesosphere.dcos.cassandra.common.offer.ClusterTaskOfferRequirementProvider;
import com.mesosphere.dcos.cassandra.common.persistence.PersistenceException;
import com.mesosphere.dcos.cassandra.common.tasks.CassandraState;
import com.mesosphere.dcos.cassandra.common.tasks.ClusterTaskManager;
import com.mesosphere.dcos.cassandra.common.tasks.upgradesstable.UpgradeSSTableContext;
import com.mesosphere.dcos.cassandra.scheduler.resources.UpgradeSSTableRequest;
import com.mesosphere.sdk.scheduler.plan.DefaultPhase;
import com.mesosphere.sdk.scheduler.plan.Phase;
import com.mesosphere.sdk.scheduler.plan.Step;
import com.mesosphere.sdk.scheduler.plan.strategy.SerialStrategy;
import com.mesosphere.sdk.state.StateStore;

import java.util.*;
import java.util.stream.Collectors;

public class UpgradeSSTableManager extends ClusterTaskManager<UpgradeSSTableRequest, UpgradeSSTableContext> {
    static final String UPGRADESSTABLE_KEY = "upgradesstable";

    private final CassandraState cassandraState;
    private final ClusterTaskOfferRequirementProvider provider;

    @Inject
    public UpgradeSSTableManager(
            CassandraState cassandraState,
            ClusterTaskOfferRequirementProvider provider,
            StateStore stateStore) {
        super(stateStore, UPGRADESSTABLE_KEY, UpgradeSSTableContext.class);
        this.cassandraState = cassandraState;
        this.provider = provider;
        restore();
    }

    protected UpgradeSSTableContext toContext(UpgradeSSTableRequest request) {
        return request.toContext(cassandraState);
    }

    protected void clearTasks() throws PersistenceException {
        cassandraState.remove(cassandraState.getUpgradeSSTableTasks().keySet());
    }

    protected List<Phase> createPhases(UpgradeSSTableContext context) {
        final Set<String> nodes = new HashSet<>(context.getNodes());
        final List<String> daemons =
                new ArrayList<>(cassandraState.getDaemons().keySet());
        Collections.sort(daemons);
        List<Step> steps = daemons.stream()
                .filter(daemon -> nodes.contains(daemon))
                .map(daemon -> new UpgradeSSTableStep(daemon, cassandraState, provider, context))
                .collect(Collectors.toList());
        return Arrays.asList(new DefaultPhase("UpgradeSSTable", steps, new SerialStrategy<>(), Collections.emptyList()));
    }
}