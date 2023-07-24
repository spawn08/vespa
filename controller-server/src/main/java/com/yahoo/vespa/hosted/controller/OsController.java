// Copyright Yahoo. Licensed under the terms of the Apache 2.0 license. See LICENSE in the project root.
package com.yahoo.vespa.hosted.controller;

import com.yahoo.component.Version;
import com.yahoo.config.provision.CloudName;
import com.yahoo.transaction.Mutex;
import com.yahoo.vespa.hosted.controller.persistence.CuratorDb;
import com.yahoo.vespa.hosted.controller.versions.OsVersion;
import com.yahoo.vespa.hosted.controller.versions.OsVersionStatus;
import com.yahoo.vespa.hosted.controller.versions.OsVersionTarget;

import java.time.Instant;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.TreeSet;
import java.util.function.Function;
import java.util.logging.Logger;
import java.util.stream.Collectors;

/**
 *  A singleton owned by {@link Controller} which contains the methods and state for controlling OS upgrades.
 *
 * @author mpolden
 */
public record OsController(Controller controller) {

    private static final Logger LOG = Logger.getLogger(OsController.class.getName());

    public OsController {
        Objects.requireNonNull(controller);
    }

    /** Returns the target OS version for infrastructure in this system. The controller will drive infrastructure OS
     * upgrades to this version */
    public Optional<OsVersionTarget> target(CloudName cloud) {
        return targets().stream().filter(target -> target.osVersion().cloud().equals(cloud)).findFirst();
    }

    /** Returns all target OS versions in this system */
    public Set<OsVersionTarget> targets() {
        return curator().readOsVersionTargets();
    }

    /**
     * Set the target OS version for given cloud in this system.
     *
     * @param version The target OS version
     * @param cloud   The cloud to upgrade
     * @param force   Allow downgrades, and override pinned target (if any)
     * @param pin     Pin this version. This prevents automatic scheduling of upgrades until version is unpinned
     */
    public void upgradeTo(Version version, CloudName cloud, boolean force, boolean pin) {
        if (version.isEmpty()) {
            throw new IllegalArgumentException("Invalid version '" + version.toFullString() + "'");
        }
        if (!controller.clouds().contains(cloud)) {
            throw new IllegalArgumentException("Cloud '" + cloud + "' does not exist in this system");
        }
        Instant scheduledAt = controller.clock().instant();
        try (Mutex lock = curator().lockOsVersions()) {
            Map<CloudName, OsVersionTarget> targets = curator().readOsVersionTargets().stream()
                                                               .collect(Collectors.toMap(t -> t.osVersion().cloud(),
                                                                                          Function.identity()));

            OsVersionTarget currentTarget = targets.get(cloud);
            boolean downgrade = false;
            if (currentTarget != null) {
                boolean versionChange = !currentTarget.osVersion().version().equals(version);
                downgrade = version.isBefore(currentTarget.osVersion().version());
                if (versionChange && currentTarget.pinned() && !force) {
                    throw new IllegalArgumentException("Cannot " + (downgrade ? "downgrade" : "upgrade") + " cloud " +
                                                       cloud.value() + "' to version " + version.toFullString() +
                                                       ": Current target is pinned. Add 'force' parameter to override");
                }
                if (downgrade && !force) {
                    throw new IllegalArgumentException("Cannot downgrade cloud '" + cloud.value() + "' to version " +
                                                       version.toFullString() + ": Missing 'force' parameter");
                }
                if (!versionChange && currentTarget.pinned() == pin) return; // No change
            }

            OsVersionTarget newTarget = new OsVersionTarget(new OsVersion(version, cloud), scheduledAt, pin, downgrade);
            targets.put(cloud, newTarget);
            curator().writeOsVersionTargets(new TreeSet<>(targets.values()));
            LOG.info("Triggered OS " + (downgrade ? "downgrade" : "upgrade") + " to " + version.toFullString() +
                     " in cloud " + cloud.value());
        }
    }

    /** Clear the target OS version for given cloud in this system */
    public void cancelUpgrade(CloudName cloudName) {
        try (Mutex lock = curator().lockOsVersions()) {
            Map<CloudName, OsVersionTarget> targets = curator().readOsVersionTargets().stream()
                                                               .collect(Collectors.toMap(t -> t.osVersion().cloud(),
                                                                                          Function.identity()));
            if (targets.remove(cloudName) == null) {
                throw new IllegalArgumentException("Cloud '" + cloudName.value() + " has no OS upgrade target");
            }
            curator().writeOsVersionTargets(new TreeSet<>(targets.values()));
        }
    }

    /** Returns the current OS version status */
    public OsVersionStatus status() {
        return curator().readOsVersionStatus();
    }

    /** Replace the current OS version status with a new one */
    public void updateStatus(OsVersionStatus newStatus) {
        try (Mutex lock = curator().lockOsVersionStatus()) {
            OsVersionStatus currentStatus = curator().readOsVersionStatus();
            for (CloudName cloud : controller.clouds()) {
                Set<Version> newVersions = newStatus.versionsIn(cloud);
                if (currentStatus.versionsIn(cloud).size() > 1 && newVersions.size() == 1) {
                    LOG.info("All nodes in " + cloud + " cloud upgraded to OS version " +
                             newVersions.iterator().next().toFullString());
                }
            }
            curator().writeOsVersionStatus(newStatus);
        }
    }

    private CuratorDb curator() {
        return controller.curator();
    }

}