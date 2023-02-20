package net.william278.husksync.redis;

import de.themoep.minedown.adventure.MineDown;
import io.lettuce.core.RedisClient;
import net.william278.husksync.HuskSync;
import net.william278.husksync.data.UserData;
import net.william278.husksync.player.User;
import net.william278.husksync.redis.redisdata.RedisPubSub;
import org.jetbrains.annotations.NotNull;

import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Date;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;

/**
 * Manages the connection to the Redis server, handling the caching of user data
 */
public class RedisManager {

    protected static final String KEY_NAMESPACE = "husksync:";
    protected static String clusterId = "";
    private final HuskSync plugin;
    private final String redisHost;
    private final int redisPort;
    private final String redisPassword;
    private final boolean redisUseSsl;
    private RedisImpl redisImpl;

    public RedisManager(@NotNull HuskSync plugin) {
        this.plugin = plugin;
        clusterId = plugin.getSettings().clusterId;

        // Set redis credentials
        this.redisHost = plugin.getSettings().redisHost;
        this.redisPort = plugin.getSettings().redisPort;
        this.redisPassword = plugin.getSettings().redisPassword;
        this.redisUseSsl = plugin.getSettings().redisUseSsl;
    }

    /**
     * Initialize the redis connection pool
     *
     * @return a future returning void when complete
     */
    public boolean initialize() {
        RedisClient client;
        if (redisPassword.isBlank()) {
            client = RedisClient.create("redis://" + redisHost + ":" + redisPort);
        } else {
            client = RedisClient.create("redis://" + redisPassword + "@" + redisHost + ":" + redisPort);
        }
        redisImpl = new RedisImpl(client);
        subscribe();
        return true;
    }

    private void subscribe() {

        redisImpl.getPubSubConnection(connection -> {

            connection.addListener(new RedisPubSub<>() {
                @Override
                public void message(String channel, String message) {
                    final RedisMessageType messageType = RedisMessageType.getTypeFromChannel(channel).orElse(null);
                    if (messageType != RedisMessageType.UPDATE_USER_DATA) {
                        return;
                    }

                    final RedisMessage redisMessage = RedisMessage.fromJson(message);
                    plugin.getOnlineUser(redisMessage.targetUserUuid).ifPresent(user -> {
                        final UserData userData = plugin.getDataAdapter().fromBytes(redisMessage.data);
                        user.setData(userData, plugin).thenAccept(succeeded -> {
                            if (succeeded) {
                                switch (plugin.getSettings().notificationDisplaySlot) {
                                    case CHAT -> plugin.getLocales().getLocale("data_update_complete")
                                            .ifPresent(user::sendMessage);
                                    case ACTION_BAR -> plugin.getLocales().getLocale("data_update_complete")
                                            .ifPresent(user::sendActionBar);
                                    case TOAST -> plugin.getLocales().getLocale("data_update_complete")
                                            .ifPresent(locale -> user.sendToast(locale, new MineDown(""),
                                                    "minecraft:bell", "TASK"));
                                }
                                plugin.getEventCannon().fireSyncCompleteEvent(user);
                            } else {
                                plugin.getLocales().getLocale("data_update_failed")
                                        .ifPresent(user::sendMessage);
                            }
                        });
                    });
                }
            });


            connection.async().subscribe(Arrays.stream(RedisMessageType.values())
                    .map(RedisMessageType::getMessageChannel)
                    .toArray(String[]::new));


        });
    }


    protected void sendMessage(@NotNull String channel, @NotNull String message) {
        redisImpl.getConnectionAsync(connection -> connection.publish(channel, message));
    }

    public CompletableFuture<Void> sendUserDataUpdate(@NotNull User user, @NotNull UserData userData) {
        return CompletableFuture.runAsync(() -> {
            final RedisMessage redisMessage = new RedisMessage(user.uuid, plugin.getDataAdapter().toBytes(userData));
            redisMessage.dispatch(this, RedisMessageType.UPDATE_USER_DATA);
        });
    }

    /**
     * Set a user's data to the Redis server
     *
     * @param user     the user to set data for
     * @param userData the user's data to set
     * @return a future returning void when complete
     */
    public CompletableFuture<Void> setUserData(@NotNull User user, @NotNull UserData userData) {
        return redisImpl.getBinaryConnectionAsync(connection ->
                        connection.setex(RedisKeyType.DATA_UPDATE.getKeyPrefix() + ":" + user.uuid,
                                RedisKeyType.DATA_UPDATE.timeToLive,
                                plugin.getDataAdapter().toBytes(userData)))
                .toCompletableFuture()
                .thenAccept(s ->
                        plugin.debug("[" + user.username + "] Set " + RedisKeyType.DATA_UPDATE.name()
                                + " key to redis at: " +
                                new SimpleDateFormat("mm:ss.SSS").format(new Date()))
                );
    }

    public CompletableFuture<Void> setUserServerSwitch(@NotNull User user) {
        return redisImpl.getBinaryConnectionAsync(connection ->
                        connection.setex(RedisKeyType.SERVER_SWITCH.getKeyPrefix() + ":" + user.uuid,
                                RedisKeyType.SERVER_SWITCH.timeToLive, new byte[0]))
                .toCompletableFuture()
                .thenAccept(s ->
                    plugin.debug("[" + user.username + "] Set " + RedisKeyType.SERVER_SWITCH.name()
                            + " key to redis at: " +
                            new SimpleDateFormat("mm:ss.SSS").format(new Date()))
                );
    }

    /**
     * Fetch a user's data from the Redis server and consume the key if found
     *
     * @param user The user to fetch data for
     * @return The user's data, if it's present on the database. Otherwise, an empty optional.
     */
    public CompletableFuture<Optional<UserData>> getUserData(@NotNull User user) {
        final String key = RedisKeyType.DATA_UPDATE.getKeyPrefix() + ":" + user.uuid;

        return redisImpl.getBinaryConnectionAsync(connection -> connection.getdel(key))
                .toCompletableFuture()
                .thenApply(bytes -> ((byte[]) bytes))
                .thenApply(dataByteArray -> {
                    if (dataByteArray == null) {
                        plugin.debug("[" + user.username + "] Could not read " +
                                RedisKeyType.DATA_UPDATE.name() + " key from redis at: " +
                                new SimpleDateFormat("mm:ss.SSS").format(new Date()));
                        return Optional.empty();
                    }
                    plugin.debug("[" + user.username + "] Successfully read "
                            + RedisKeyType.DATA_UPDATE.name() + " key from redis at: " +
                            new SimpleDateFormat("mm:ss.SSS").format(new Date()));

                    // Use the data adapter to convert the byte array to a UserData object

                    final UserData userData = plugin.getDataAdapter().fromBytes(dataByteArray);
                    return Optional.of(userData);
                });
    }

    public CompletableFuture<Boolean> getUserServerSwitch(@NotNull User user) {
        final String key = getKey(RedisKeyType.SERVER_SWITCH, user.uuid);

        return redisImpl.getBinaryConnection(connection -> connection.getdel(key))
                .toCompletableFuture()
                .thenApply(bytes -> ((byte[]) bytes))
                .thenApply(dataByteArray -> {
                    if (dataByteArray == null) {
                        plugin.debug("[" + user.username + "] Could not read " +
                                RedisKeyType.SERVER_SWITCH.name() + " key from redis at: " +
                                new SimpleDateFormat("mm:ss.SSS").format(new Date()));
                        return false;
                    }
                    plugin.debug("[" + user.username + "] Successfully read "
                            + RedisKeyType.SERVER_SWITCH.name() + " key from redis at: " +
                            new SimpleDateFormat("mm:ss.SSS").format(new Date()));

                    return true;
                });
    }

    public void close() {
        redisImpl.close();
    }

    private static String getKey(@NotNull RedisKeyType keyType, @NotNull UUID uuid) {
        return (keyType.getKeyPrefix() + ":" + uuid);
    }

}
