package com.mesosphere.dcos.cassandra.scheduler.plan.backup;

import com.google.inject.Inject;
import com.mesosphere.dcos.cassandra.common.offer.ClusterTaskOfferRequirementProvider;
import com.mesosphere.dcos.cassandra.common.persistence.PersistenceException;
import com.mesosphere.dcos.cassandra.common.tasks.CassandraState;
import com.mesosphere.dcos.cassandra.common.tasks.ClusterTaskManager;
import com.mesosphere.dcos.cassandra.common.tasks.backup.BackupRestoreContext;
import com.mesosphere.dcos.cassandra.scheduler.resources.BackupRestoreRequest;
import com.mesosphere.sdk.scheduler.plan.DefaultPhase;
import com.mesosphere.sdk.scheduler.plan.Phase;
import com.mesosphere.sdk.scheduler.plan.Step;
import com.mesosphere.sdk.scheduler.plan.strategy.SerialStrategy;
import com.mesosphere.sdk.state.StateStore;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

public class RestoreManager extends ClusterTaskManager<BackupRestoreRequest, BackupRestoreContext> {
    static final String RESTORE_KEY = "restore";

    private CassandraState cassandraState;
    private final ClusterTaskOfferRequirementProvider provider;

    @Inject
    public RestoreManager(
            final CassandraState cassandraState,
            final ClusterTaskOfferRequirementProvider provider,
            StateStore stateStore) {
        super(stateStore, RESTORE_KEY, BackupRestoreContext.class);
        this.provider = provider;
        this.cassandraState = cassandraState;
        restore();
    }

    @Override
    protected BackupRestoreContext toContext(BackupRestoreRequest request) {
        return request.toContext();
    }

    @Override
    protected List<Phase> createPhases(BackupRestoreContext context) {
        return Arrays.asList(
                createRestoreSchemaPhase(context, cassandraState, provider),
                createDownloadSnapshotPhase(context, cassandraState, provider),
                createRestoreSnapshotPhase(context, cassandraState, provider));
    }

    @Override
    protected void clearTasks() throws PersistenceException {
        cassandraState.remove(cassandraState.getRestoreSchemaTasks().keySet());
        cassandraState.remove(cassandraState.getDownloadSnapshotTasks().keySet());
        cassandraState.remove(cassandraState.getRestoreSnapshotTasks().keySet());
    }

    private static Phase createRestoreSchemaPhase(
            BackupRestoreContext context,
            CassandraState cassandraState,
            ClusterTaskOfferRequirementProvider provider) {
        final List<String> daemons = new ArrayList<>(cassandraState.getDaemons().keySet());
        Collections.sort(daemons);
        List<Step> steps = daemons.stream()
                .map(daemon -> new RestoreSchemaStep(daemon, cassandraState, provider, context))
                .collect(Collectors.toList());
        return new DefaultPhase("RestoreSchema", steps, new SerialStrategy<>(), Collections.emptyList());
    }

    private static Phase createDownloadSnapshotPhase(
            BackupRestoreContext context,
            CassandraState cassandraState,
            ClusterTaskOfferRequirementProvider provider) {
        final List<String> daemons = new ArrayList<>(cassandraState.getDaemons().keySet());
        Collections.sort(daemons);
        List<Step> steps = daemons.stream()
                .map(daemon -> new DownloadSnapshotStep(daemon, cassandraState, provider, context))
                .collect(Collectors.toList());
        return new DefaultPhase("Download", steps, new SerialStrategy<>(), Collections.emptyList());
    }

    private static Phase createRestoreSnapshotPhase(
            BackupRestoreContext context,
            CassandraState cassandraState,
            ClusterTaskOfferRequirementProvider provider) {
        final List<String> daemons = new ArrayList<>(cassandraState.getDaemons().keySet());
        Collections.sort(daemons);
        List<Step> steps = daemons.stream()
                .map(daemon -> new RestoreSnapshotStep(daemon, cassandraState, provider, context))
                .collect(Collectors.toList());
        return new DefaultPhase("Restore", steps, new SerialStrategy<>(), Collections.emptyList());
    }
}