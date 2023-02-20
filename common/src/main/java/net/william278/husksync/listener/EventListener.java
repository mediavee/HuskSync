package net.william278.husksync.listener;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import de.themoep.minedown.adventure.MineDown;
import net.william278.husksync.HuskSync;
import net.william278.husksync.data.DataSaveCause;
import net.william278.husksync.data.ItemData;
import net.william278.husksync.player.OnlineUser;
import net.william278.husksync.util.NamedThreadPoolFactory;
import org.jetbrains.annotations.NotNull;

import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Level;

/**
 * Handles what should happen when events are fired
 */
public abstract class EventListener {

    /**
     * The plugin instance
     */
    protected final HuskSync plugin;

    /**
     * Set of UUIDs of "locked players", for which events will be cancelled.
     * </p>
     * Players are locked while their items are being set (on join) or saved (on quit)
     */
    private final Set<UUID> lockedPlayers;

    /**
     * Whether the plugin is currently being disabled
     */
    private boolean disabling;

    public static ForkJoinPool executor;

    static {
        RuntimeException ex = new RuntimeException();
        StackTraceElement[] stackTrace = ex.getStackTrace();
        if (stackTrace.length > 0) {
            StackTraceElement element = stackTrace[0];
            Throwable throwable = new Throwable();
            throwable.setStackTrace(new StackTraceElement[]{element});
            throwable.printStackTrace();
        }

        ForkJoinPool.ForkJoinWorkerThreadFactory factory = pool -> {
            final ForkJoinWorkerThread worker = ForkJoinPool.defaultForkJoinWorkerThreadFactory.newThread(pool);
            worker.setName("HuskSync-EventListener-" + worker.getPoolIndex());
            return worker;
        };
        executor = new ForkJoinPool(
                8,
                factory,
                null,
                true,
                0,
                8,
                0,
                null,
                60_000L,
                TimeUnit.MILLISECONDS
        );
    }

    protected EventListener(@NotNull HuskSync plugin) {

        this.plugin = plugin;
        this.lockedPlayers = new HashSet<>();
        this.disabling = false;
    }

    /**
     * Handle a player joining the server (including players switching from another proxied server)
     *
     * @param user The {@link OnlineUser} to handle
     */
    protected final void handlePlayerJoin(@NotNull OnlineUser user) {
        if (user.isNpc()) {
            return;
        }

        lockedPlayers.add(user.uuid);
        CompletableFuture.runAsync(() -> {
            plugin.getRedisManager().getUserServerSwitch(user).thenAccept(changingServers -> {
                if (!changingServers) {
                    // Fetch from the database if the user isn't changing servers
                    setUserFromDatabase(user)
                            .thenAccept(succeeded -> handleSynchronisationCompletion(user, succeeded));
                } else {
                    final int TIME_OUT_MILLISECONDS = 3200;
                    final AtomicInteger currentMilliseconds = new AtomicInteger(0);
                    final ScheduledExecutorService scheduledExecutor = Executors.newSingleThreadScheduledExecutor();

                    // Set the user as soon as the source server has set the data to redis
                    scheduledExecutor.scheduleAtFixedRate(() -> {
                        if (user.isOffline()) {
                            scheduledExecutor.shutdown();
                            return;
                        }
                        if (disabling || currentMilliseconds.get() > TIME_OUT_MILLISECONDS) {
                            scheduledExecutor.shutdown();
                            setUserFromDatabase(user)
                                    .thenAccept(succeeded -> handleSynchronisationCompletion(user, succeeded));
                            return;
                        }
                        plugin.getRedisManager().getUserData(user).thenAccept(redisUserData ->
                                redisUserData.ifPresent(redisData -> {
                                    user.setData(redisData, plugin)
                                            .thenAccept(succeeded -> handleSynchronisationCompletion(user, succeeded));
                                    scheduledExecutor.shutdown();
                                })).thenRun(() -> currentMilliseconds.addAndGet(200));
                    }, 0, 200L, TimeUnit.MILLISECONDS);
                }
            });
        }, CompletableFuture.delayedExecutor(plugin.getSettings().networkLatencyMilliseconds, TimeUnit.MILLISECONDS, executor));
    }



    /**
     * Set a user's data from the database
     *
     * @param user The user to set the data for
     * @return Whether the data was successfully set
     */
    private CompletableFuture<Boolean> setUserFromDatabase(@NotNull OnlineUser user) {
        return plugin.getDatabase().getCurrentUserData(user).thenApply(databaseUserData -> {
            if (databaseUserData.isPresent()) {
                return user.setData(databaseUserData.get().userData(), plugin).join();
            }
            return true;
        });
    }

    /**
     * Handle a player's synchronization completion
     *
     * @param user      The {@link OnlineUser} to handle
     * @param succeeded Whether the synchronization succeeded
     */
    private void handleSynchronisationCompletion(@NotNull OnlineUser user, boolean succeeded) {
        if (succeeded) {
            switch (plugin.getSettings().notificationDisplaySlot) {
                case CHAT -> plugin.getLocales().getLocale("synchronisation_complete")
                        .ifPresent(user::sendMessage);
                case ACTION_BAR -> plugin.getLocales().getLocale("synchronisation_complete")
                        .ifPresent(user::sendActionBar);
                case TOAST -> plugin.getLocales().getLocale("synchronisation_complete")
                        .ifPresent(locale -> user.sendToast(locale, new MineDown(""),
                                "minecraft:bell", "TASK"));
            }
            plugin.getDatabase().ensureUser(user).thenRun(() -> {
                plugin.getEventCannon().fireSyncCompleteEvent(user);
                lockedPlayers.remove(user.uuid);
            });
        } else {
            plugin.getLocales().getLocale("synchronisation_failed")
                    .ifPresent(user::sendMessage);
            plugin.getDatabase().ensureUser(user);
        }
    }

    /**
     * Handle a player leaving the server (including players switching to another proxied server)
     *
     * @param user The {@link OnlineUser} to handle
     */
    protected final void handlePlayerQuit(@NotNull OnlineUser user) {
        // Players quitting have their data manually saved by the plugin disable hook
        System.out.println("handlePlayerQuit");
        if (disabling) {
            return;
        }
        // Don't sync players awaiting synchronization
        System.out.println("check if player is locked");
        if (lockedPlayers.contains(user.uuid) || user.isNpc()) {
            return;
        }

        // Handle asynchronous disconnection
        System.out.println("handle async disconnection");
        lockedPlayers.add(user.uuid);
        plugin.getRedisManager().setUserServerSwitch(user)
                .thenRun(() -> {
                    System.out.println("set user server switch");
                    user.getUserData(plugin).thenAccept(userData -> {
                        System.out.println("get user data");
                        userData.ifPresent(userDataGetted -> {
                            System.out.println("set user data");
                            plugin.getRedisManager().setUserData(user, userDataGetted).thenRun(() -> {
                                System.out.println("set user data in redis");
                                plugin.getDatabase().setUserData(user, userDataGetted, DataSaveCause.DISCONNECT);
                                System.out.println("set user data in database");
                            });
                        });
                    });
                })
                .exceptionally(throwable -> {
                    plugin.log(Level.SEVERE,
                            "An exception occurred handling a player disconnection");
                    throwable.printStackTrace();
                    return null;
                });
        System.out.println("end of handlePlayerQuit");
    }

    /**
     * Handles the saving of data when the world save event is fired
     *
     * @param usersInWorld a list of users in the world that is being saved
     */
    protected final void saveOnWorldSave(@NotNull List<OnlineUser> usersInWorld) {
        if (disabling || !plugin.getSettings().saveOnWorldSave) {
            return;
        }
        usersInWorld.stream()
                .filter(user -> !lockedPlayers.contains(user.uuid) && !user.isNpc())
                .forEach(user -> user.getUserData(plugin)
                        .thenAccept(data -> data.ifPresent(userData -> plugin.getDatabase()
                                .setUserData(user, userData, DataSaveCause.WORLD_SAVE))));
    }

    /**
     * Handles the saving of data when a player dies
     *
     * @param user  The user who died
     * @param drops The items that this user would have dropped
     */
    protected void saveOnPlayerDeath(@NotNull OnlineUser user, @NotNull ItemData drops) {
        if (disabling || !plugin.getSettings().saveOnDeath || lockedPlayers.contains(user.uuid) || user.isNpc()) {
            return;
        }

        user.getUserData(plugin)
                .thenAccept(data -> data.ifPresent(userData -> {
                    userData.getInventory().orElse(ItemData.empty()).serializedItems = drops.serializedItems;
                    plugin.getDatabase().setUserData(user, userData, DataSaveCause.DEATH);
                }));
    }

    /**
     * Determine whether a player event should be cancelled
     *
     * @param userUuid The UUID of the user to check
     * @return Whether the event should be cancelled
     */
    protected final boolean cancelPlayerEvent(@NotNull UUID userUuid) {
        return disabling || lockedPlayers.contains(userUuid);
    }

    /**
     * Handle the plugin disabling
     */
    public final void handlePluginDisable() {
        disabling = true;

        // Save data for all online users
        plugin.getOnlineUsers().stream()
                .filter(user -> !lockedPlayers.contains(user.uuid) && !user.isNpc())
                .forEach(user -> {
                    lockedPlayers.add(user.uuid);
                    user.getUserData(plugin).join()
                            .ifPresent(userData -> plugin.getDatabase()
                                    .setUserData(user, userData, DataSaveCause.SERVER_SHUTDOWN).join());
                });

        // Close outstanding connections
        plugin.getDatabase().close();
        plugin.getRedisManager().close();
    }

    public final Set<UUID> getLockedPlayers() {
        return this.lockedPlayers;
    }

    public ForkJoinPool getExecutor() {
        return executor;
    }
}
