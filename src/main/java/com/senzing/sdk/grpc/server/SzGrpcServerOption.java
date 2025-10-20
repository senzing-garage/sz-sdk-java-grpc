package com.senzing.sdk.grpc.server;

import java.io.File;
import java.io.IOException;
import java.net.InetAddress;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;

import javax.json.JsonObjectBuilder;

import java.util.List;

import com.senzing.cmdline.CommandLineException;
import com.senzing.cmdline.CommandLineOption;
import com.senzing.cmdline.CommandLineUtilities;
import com.senzing.cmdline.CommandLineValue;
import com.senzing.cmdline.DeprecatedOptionWarning;
import com.senzing.cmdline.ParameterProcessor;
import com.senzing.util.JsonUtilities;

import static com.senzing.sdk.grpc.server.SzGrpcServerConstants.*;
import static com.senzing.util.CollectionUtilities.*;
import static com.senzing.util.LoggingUtilities.*;
import static com.senzing.io.IOUtilities.*;

/**
 * Enumerates the options to the {@link SzGrpcServer}.
 */
@SuppressWarnings("rawtypes")
public enum SzGrpcServerOption
    implements CommandLineOption<SzGrpcServerOption, SzGrpcServerOption> 
{
    /**
     * <p>
     * Option for displaying help/usage for the gRPC Server. This option can
     * only be provided by itself and has no parameters.
     * <p>
     * This option can be specified in the following ways:
     * <ul>
     * <li>Command Line: <code>-h</code></li>
     * <li>Command Line: <code>--help</code></li>
     * </ul>
     */
    HELP("--help", Set.of("-h"), true, 0),

    /**
     * <p>
     * Option for displaying the version number of the gRPC Server. This option
     * can only be provided by itself and has no parameters.
     * <p>
     * This option can be specified in the following ways:
     * <ul>
     * <li>Command Line: <code>--v</code></li>
     * <li>Command Line: <code>--version</code></li>
     * </ul>
     */
    VERSION("--version", Set.of("-v"), true, 0),

    /**
     * <p>
     * Option for ignoring environment variables when setting the values for
     * other command-line options.  A single parameter may optionally be
     * specified as <code>true</code> or <code>false</code> with
     * <code>false</code> simulating the absence of the option.
     * <p>
     * This option can be specified in the following ways:
     * <ul>
     * <li>Command Line: <code>--ignore-environment [true|false]</code></li>
     * </ul>
     */
    IGNORE_ENVIRONMENT("--ignore-environment", 
                       null, 0,
                        "false"),

    /**
     * <p>
     * Option for specifying the port for the gRPC Server. This has a single
     * parameter which can be a positive integer port number or can be zero (0)
     * to indicate binding to a randomly selected port number. If not provided
     * then {@link SzGrpcServerConstants#DEFAULT_PORT_PARAM} is used.
     * <p>
     * This option can be specified in the following ways:
     * <ul>
     * <li>Command Line: <code>-p {positive-port-number|0}</code></li>
     * <li>Command Line: <code>--grpc-port {positive-port-number|0}</code></li>
     * <li>Environment:
     * <code>SENZING_TOOLS_GRPC_PORT="{positive-port-number|0}"</code></li>
     * </ul>
     */
    GRPC_PORT("--grpc-port", ENV_PREFIX + "GRPC_PORT",
              1, DEFAULT_PORT_PARAM),

    /**
     * <p>
     * Option for specifying the server bind address for the gRPC Server. The
     * possible values can be an actual network interface name, an IP address,
     * the word <code>"loopback"</code> for the loopback local address or
     * <code>"all"</code>
     * to indicate all configured network interfaces. The default value is
     * {@link SzGrpcServerConstants#DEFAULT_BIND_ADDRESS}.
     * <p>
     * This option can be specified in the following ways:
     * <ul>
     * <li>Command Line:
     * <code>--server-address {ip-address|loopback|all}</code></li>
     * <li>Environment:
     * <code>SENZING_TOOLS_SERVER_ADDRESS="{ip-address|loopback|all}"</code></li>
     * </ul>
     */
    BIND_ADDRESS("--server-address", ENV_PREFIX + "SERVER_ADDRESS",
                   1, DEFAULT_BIND_ADDRESS_PARAM),

    /**
     * <p>
     * Option for specifying the module name to initialize the Senzing API's
     * with. The default value is {@link
     * SzGrpcServerConstants#DEFAULT_INSTANCE_NAME}.
     * <p>
     * This option can be specified in the following ways:
     * <ul>
     * <li>Command Line: <code>-n {module-name}</code></li>
     * <li>Command Line: <code>--instance-name {module-name}</code></li>
     * <li>Environment:
     * <code>SENZING_TOOLS_CORE_INSTANCE_NAME="{module-name}"</code></li>
     * </ul>
     */
    CORE_INSTANCE_NAME("--core-instance-name", 
                       ENV_PREFIX + "CORE_INSTANCE_NAME",
                       1, DEFAULT_INSTANCE_NAME),

    /**
     * <p>
     * Option for specifying the core settings JSON with which to initialize
     * the Core Senzing SDK. The parameter to this option should be the
     * settings as a JSON object <b>or</b> the path to a file containing the
     * settings JSON.
     * <p>
     * This option can be specified in the following ways:
     * <ul>
     * <li>Command Line: <code>--core-settings [{file-path}|{json-text}]</code></li>
     * <li>Environment:
     * <code>SENZING_TOOLS_CORE_SETTINGS="[{file-path}|{json-text}]"</code></li>
     * </ul>
     */
    CORE_SETTINGS("--core-settings",
                  ENV_PREFIX + "CORE_SETTINGS",
                  List.of("SENZING_ENGINE_CONFIGURATION_JSON"),
                  true, 1),

    /**
     * <p>
     * This option is used with {@link #CORE_SETTINGS} to force a specific
     * configuration ID to be used for initialization and prevent automatic
     * reinitialization to pickup the latest default config ID.
     * <p>
     * This option can be specified in the following ways:
     * <ul>
     * <li>Command Line: <code>--config-id {config-id}</code></li>
     * <li>Environment: <code>SENZING_TOOLS_CORE_CONFIG_ID="{config-id}"</code></li>
     * </ul>
     */
    CORE_CONFIG_ID("--core-config-id",
                   ENV_PREFIX + "CORE_CONFIG_ID", 1),

    /**
     * <p>
     * This presence of this option determines if the Core Senzing SDK
     * is initialized in verbose mode. The default value if not specified
     * is <code>muted</code> (which is equivalent to zero). The parameter
     * to this option may be specified as one of:
     * <ul>
     * <li><code>muted</code> - To indicate no logging.</li>
     * <li><code>verbose</code> - To indicate verbose logging.</li>
     * <li><code>0</code> - To indicate no logging.</li>
     * <li><code>1</code> - To indicate verbose logging.</li>
     * </ul>
     * <p>
     * This option can be specified in the following ways:
     * <ul>
     * <li>Command Line:
     * <code>--core-log-level [muted|verbose|{integer}]</code></li>
     * <li>Environment:
     * <code>SENZING_TOOLS_CORE_LOG_LEVEL="[muted|verbose|{integer}]"</code></li>
     * </ul>
     */
    CORE_LOG_LEVEL("--core-log-level",
                   ENV_PREFIX + "CORE_LOG_LEVEL",
                   0, "muted"),

    /**
     * <p>
     * This option sets the number of threads available for executing
     * Core Senzing SDK functions. The single parameter to this option
     * should be a positive integer. If not specified, then this
     * defaults to {@link SzGrpcServerConstants#DEFAULT_CORE_CONCURRENCY},
     * <p>
     * This option can be specified in the following ways:
     * <ul>
     * <li>Command Line: <code>--core-concurrency {thread-count}</code></li>
     * <li>Environment:
     * <code>SENZING_TOOLS_CORE_CONCURRENCY="{thread-count}"</code></li>
     * </ul>
     */
    CORE_CONCURRENCY("--core-concurrency",
                     ENV_PREFIX + "CORE_CONCURRENCY",
                     1, DEFAULT_CORE_CONCURRENCY_PARAM),

    /**
     * <p>
     * This option sets the number of threads available for executing Senzing API
     * functions (i.e.: the number of engine threads). The single parameter to
     * this option should be a positive integer. If not specified, then this
     * defaults to {@link SzGrpcServerConstants#DEFAULT_GRPC_CONCURRENCY},
     * <p>
     * This option can be specified in the following ways:
     * <ul>
     * <li>Command Line: <code>--core-concurrency {thread-count}</code></li>
     * <li>Environment:
     * <code>SENZING_TOOLS_CORE_CONCURRENCY="{thread-count}"</code></li>
     * </ul>
     */
    GRPC_CONCURRENCY("--grpc-concurrency",
                     ENV_PREFIX + "GRPC_CONCURRENCY",
                     1, DEFAULT_GRPC_CONCURRENCY_PARAM),

    /**
     * <p>
     * If leveraging the default configuration stored in the database, this option
     * is used to specify how often the gRPC server should background check that
     * the current active config is the same as the current default config and
     * update the active config if not. The parameter to this option is specified
     * as an integer:
     * <ul>
     * <li>A positive integer is interpreted as a number of seconds.</li>
     * <li>If zero is specified, the auto-refresh is disabled and it will
     * only occur when a requested configuration element is not found
     * in the current active config.</li>
     * <li>Specifying a negative integer is allowed but is used to enable
     * a check and conditional refresh only when manually requested
     * (programmatically).</li>
     * </ul>
     * <b>NOTE:</b> This is option ignored if auto-refresh is disabled because
     * the config was specified via the <code>G2CONFIGFILE</code> in the
     * {@link #CORE_SETTINGS} or if {@link #CORE_CONFIG_ID} has been specified
     * to lock in a specific configuration.
     * <p>
     * This option can be specified in the following ways:
     * <ul>
     * <li>Command Line:
     * <code>--refresh-config-seconds {integer}</code></li>
     * <li>Command Line:
     * <li>Environment:
     * <code>SENZING_TOOLS_REFRESH_CONFIG_SECONDS="{integer}"</code></li>
     * </ul>
     */
    REFRESH_CONFIG_SECONDS("--refresh-config-seconds",
                           ENV_PREFIX + "REFRESH_CONFIG_SECONDS",
                           1, DEFAULT_REFRESH_CONFIG_SECONDS_PARAM),

    /**
     * <p>
     * This option is used to specify the minimum number of <b>seconds</b>
     * between logging of stats. This is minimum because stats logging is
     * suppressed if the gRPC Server is idle or active but not performing
     * activities pertaining to entity scoring. In such cases, stats logging is
     * delayed until an activity pertaining to entity scoring is performed. By
     * default this is set to
     * {@link SzGrpcServerConstants#DEFAULT_LOG_STATS_SECONDS}.
     * If zero (0) is specified then the logging of stats will be suppressed.
     * <p>
     * This option can be specified in the following ways:
     * <ul>
     * <li>Command Line: <code>--log-stats-seconds {seconds}</code></li>
     * <li>Environment:
     * <code>SENZING_TOOLS_LOG_STATS_SECONDS="{seconds}"</code></li>
     * </ul>
     */
    LOG_STATS_SECONDS("--log-stats-seconds",
                      ENV_PREFIX + "LOG_STATS_SECONDS",
                      1, DEFAULT_LOG_STATS_SECONDS_PARAM),

    /**
     * <p>
     * The presence of this option causes the gRPC Server to skip a performance
     * check on startup, and its absence allows the performance check to be
     * performed as normal. A single parameter may optionally be specified as
     * <code>true</code> or <code>false</code> with <code>false</code> simulating
     * the absence of the option.
     * <p>
     * This option can be specified in the following ways:
     * <ul>
     * <li>Command Line: <code>--skip-startup-perf [true|false]</code></li>
     * <li>Environment:
     * <code>SENZING_TOOLS_SKIP_STARTUP_PERF="{true|false}"</code></li>
     * </ul>
     */
    SKIP_STARTUP_PERF("--skip-startup-perf",
                      ENV_PREFIX + "SKIP_STARTUP_PERF",
                      0, "false"),

    /**
     * The presence of this option causes the API Server to skip priming the
     * engine on startup, and its absence allows the engine priming to occur as
     * is the default behavior. A single parameter may optionally be specified as
     * <code>true</code> or <code>false</code> with <code>false</code> simulating
     * the absence
     * of the option.
     * <ul>
     * <li>Command Line: <code>--skip-engine-priming [true|false]</code></li>
     * <li>Environment:
     * <code>SENZING_TOOLS_SKIP_ENGINE_PRIMING="{true|false}"</code></li>
     * </ul>
     */
    SKIP_ENGINE_PRIMING("--skip-engine-priming",
                        ENV_PREFIX + "SKIP_ENGINE_PRIMING",
                        0, "false");

    /**
     * The {@link Map} of {@link SzGrpcServerOption} keys to unmodifiable
     * {@link Set} values containing the {@link SzGrpcServerOption} values that
     * conflict with the key {@link SzGrpcServerOption} value.
     */
    private static Map<SzGrpcServerOption, Set<CommandLineOption>> CONFLICTING_OPTIONS;

    /**
     * The {@link Map} of {@link SzGrpcServerOption} keys to <b>unmodifiable</b>
     * {@link Set} values containing the {@link SzGrpcServerOption} values that
     * are alternatives to the key {@link SzGrpcServerOption} value.
     */
    private static Map<SzGrpcServerOption, Set<SzGrpcServerOption>> ALTERNATIVE_OPTIONS;

    /**
     * The {@link Map} of {@link String} option flags to their corresponding
     * {@link SzGrpcServerOption} values.
     */
    private static Map<String, SzGrpcServerOption> OPTIONS_BY_FLAG;

    /**
     * The {@link Map} of {@link SzGrpcServerOption} keys to <b>unmodifiable</b>
     * {@link Set} values containing alternative {@link Set}'s of {@link
     * SzGrpcServerOption} that the key option is dependent on if specified.
     */
    private static Map<SzGrpcServerOption, Set<Set<CommandLineOption>>> DEPENDENCIES;

    /**
     * Flag indicating if this option is considered a "primary" option.
     */
    private boolean primary;

    /**
     * Flag indicating if this option is considered a deprecated option.
     */
    private boolean deprecated;

    /**
     * The primary command-line flag.
     */
    private String cmdLineFlag;

    /**
     * The {@link Set} of synonym command-line flags for this option.
     */
    private Set<String> synonymFlags;

    /**
     * The optional environment variable associated with option.
     */
    private String envVariable;

    /**
     * The environment variable fallbacks for this option.
     */
    private List<String> envFallbacks;

    /**
     * The minimum number of parameters that can be specified for this option.
     */
    private int minParamCount;

    /**
     * The maximum number of parameters that can be specified for this option.
     */
    private int maxParamCount;

    /**
     * The {@link List} o {@link String} default parameters for this option. This
     * is <code>null</code> if no default and an empty {@link List} if the option is
     * specified by default with no parameters.
     */
    private List<String> defaultParameters;

    /**
     * The group name for the option group that this parameter belongs to.
     */
    private String groupName;

    /**
     * The property key to map the option to for the group for initializing a
     * sub-object with the options in that group.
     */
    private String groupPropertyKey;

    /**
     * The property indicating if the option is not required for the validity of
     * the group to which it belongs.
     */
    private boolean groupOptional;

    SzGrpcServerOption(
            String cmdLineFlag,
            Set<String> synonymFlags,
            boolean primary,
            int parameterCount)
    {
        this(cmdLineFlag,
             synonymFlags,
             null,
             null,
             primary,
             parameterCount,
             Collections.emptyList(),
             false,
             null,
             null,
             true);
    }
    
    SzGrpcServerOption(
            String cmdLineFlag,
            String envVariable,
            int parameterCount,
            String... defaultParams) 
    {
        this(cmdLineFlag,
             Collections.emptySet(),
             envVariable,
             null,
             false,
             parameterCount,
             List.of(defaultParams),
             false,
             null,
             null,
             true);
    }

    SzGrpcServerOption(
            String cmdLineFlag,
            String envVariable,
            List<String> envFallbacks,
            int parameterCount,
            String... defaultParams) 
    {
        this(cmdLineFlag,
             Collections.emptySet(),
             envVariable,
             envFallbacks,
             false,
             parameterCount,
             List.of(defaultParams),
             false,
             null,
             null,
             true);
    }

    SzGrpcServerOption(String cmdLineFlag,
            String envVariable,
            List<String> envFallbacks,
            boolean primary,
            int parameterCount) {
        this(cmdLineFlag,
             Collections.emptySet(),
             envVariable,
             envFallbacks,
             primary,
             parameterCount < 0 ? 0 : parameterCount,
             parameterCount,
             Collections.emptyList(),
             false,
             null,
             null,
            true);
    }

    SzGrpcServerOption(
            String          cmdLineFlag,
            Set<String>     synonymFlags,
            String          envVariable,
            List<String>    envFallbacks,
            boolean         primary,
            int             parameterCount,
            List<String>    defaultParameters,
            boolean         deprecated,
            String          groupName,
            String          groupPropertyKey,
            boolean         groupOptional) 
    {
        this(cmdLineFlag, 
             synonymFlags,
             envVariable,
             envFallbacks,
             primary,
             parameterCount,
             parameterCount,
             defaultParameters,
             deprecated,
             groupName,
             groupPropertyKey,
             groupOptional);
    }

    SzGrpcServerOption(
            String          cmdLineFlag,
            Set<String>     synonymFlags,
            String          envVariable,
            List<String>    envFallbacks,
            boolean         primary,
            int             minParameterCount,
            int             maxParameterCount,
            List<String>    defaultParameters,
            boolean         deprecated,
            String          groupName,
            String          groupPropertyKey,
            boolean         groupOptional) 
    {
        this.cmdLineFlag = cmdLineFlag;
        this.synonymFlags = Set.copyOf(synonymFlags);
        this.envVariable = envVariable;
        this.primary = primary;
        this.minParamCount = minParameterCount;
        this.maxParamCount = maxParameterCount;
        this.deprecated = deprecated;
        this.groupName = groupName;
        this.groupPropertyKey = groupPropertyKey;
        this.groupOptional = groupOptional;
        this.envFallbacks = (envFallbacks == null)
                ? Collections.emptyList()
                : List.copyOf(envFallbacks);
        this.defaultParameters = (defaultParameters == null)
                ? Collections.emptyList()
                : List.copyOf(defaultParameters);
    }

    @Override
    public int getMinimumParameterCount() {
        return this.minParamCount;
    }

    @Override
    public int getMaximumParameterCount() {
        return this.maxParamCount;
    }

    @Override
    public List<String> getDefaultParameters() {
        return this.defaultParameters;
    }

    public boolean isPrimary() {
        return this.primary;
    }

    @Override
    public boolean isDeprecated() {
        return this.deprecated;
    }

    @Override
    public String getCommandLineFlag() {
        return this.cmdLineFlag;
    }

    @Override
    public Set<String> getSynonymFlags() {
        return this.synonymFlags;
    }

    @Override
    public String getEnvironmentVariable() {
        return this.envVariable;
    }

    @Override
    public List<String> getEnvironmentFallbacks() {
        return this.envFallbacks;
    }

    /**
     * Gets the group name (identifier) if this option is 
     * grouped with other options.
     * 
     * @return The group name (identifier) for this option,
     *         or <code>null</code> if this option does not
     *         belong to a group.
     */
    public String getGroupName() {
        return this.groupName;
    }

    /**
     * The key under which the option value is stored in the
     * option group's {@link Map} of properties.
     * 
     * @return The key under which the option value is stored
     *         in the option group's {@link Map} of properties.
     */
    public String getGroupPropertyKey() {
        return this.groupPropertyKey;
    }

    /**
     * Checks if this option is optional for the group.  These 
     * options will still trigger the required options for the
     * group to specified, but are not required if one of the
     * required options for the group is specified.
     * 
     * @return <code>true</code> if the option is optional for 
     *         for the group, otherwise <code>false</code>.
     */
    public boolean isGroupOptional() {
        return this.groupOptional;
    }

    @Override
    public Set<CommandLineOption> getConflicts() {
        return CONFLICTING_OPTIONS.get(this);
    }

    /**
     * Gets the alternative options for this option when there 
     * are several options that allow different ways to specify 
     * a similar thing and only one can be specified.
     * 
     * @return The <b>unmodifiable</b> {@link Set} of alternative
     *         options for this option.
     */
    public Set<SzGrpcServerOption> getAlternatives() {
        return ALTERNATIVE_OPTIONS.get(this);
    }

    @Override
    public Set<Set<CommandLineOption>> getDependencies() {
        Set<Set<CommandLineOption>> set = DEPENDENCIES.get(this);
        return (set == null) ? Collections.emptySet() : set;
    }

    /**
     * Finds the {@link SzGrpcServerOption} corresponding to the 
     * specified command-line flag.  This returns <code>null</code>
     * if none is found.
     * 
     * @param commandLineFlag The command-line flag for which the 
     *                        {@link SzGrpcServerOption} is being 
     *                        requested.
     * 
     * @return The corresponding {@link SzGrpcServerOption} or
     *         <code>null</code> if none exists for the flag.
     */
    public static SzGrpcServerOption lookup(String commandLineFlag) {
        return OPTIONS_BY_FLAG.get(commandLineFlag.toLowerCase());
    }

    static {
        try {
            Map<SzGrpcServerOption, Set<CommandLineOption>> conflictMap = new LinkedHashMap<>();
            Map<SzGrpcServerOption, Set<SzGrpcServerOption>> altMap = new LinkedHashMap<>();
            Map<String, SzGrpcServerOption> lookupMap = new LinkedHashMap<>();

            for (SzGrpcServerOption option : SzGrpcServerOption.values()) {
                conflictMap.put(option, new LinkedHashSet<>());
                altMap.put(option, new LinkedHashSet<>());
                lookupMap.put(option.getCommandLineFlag().toLowerCase(), option);
            }
            SzGrpcServerOption[] exclusiveOptions = { HELP, VERSION };
            for (SzGrpcServerOption option : SzGrpcServerOption.values()) {
                for (SzGrpcServerOption exclOption : exclusiveOptions) {
                    if (option == exclOption)
                        continue;
                    Set<CommandLineOption> set = conflictMap.get(exclOption);
                    set.add(option);
                    set = conflictMap.get(option);
                    set.add(exclOption);
                }
            }

            Map<SzGrpcServerOption, Set<Set<CommandLineOption>>> dependencyMap = new LinkedHashMap<>();

            // handle dependencies for groups of options that go together
            Map<String, Set<SzGrpcServerOption>> groups = new LinkedHashMap<>();
            for (SzGrpcServerOption option : SzGrpcServerOption.values()) {
                String groupName = option.getGroupName();
                if (groupName == null)
                    continue;
                Set<SzGrpcServerOption> set = groups.get(groupName);
                if (set == null) {
                    set = new LinkedHashSet<>();
                    groups.put(groupName, set);
                }
                set.add(option);
            }

            // create the dependencies using the groupings
            groups.forEach((groupName, group) -> {
                for (SzGrpcServerOption option : group) {
                    Set<CommandLineOption> others = new LinkedHashSet<>(group);

                    // remove self from the group (can't depend on itself)
                    others.remove(option);

                    // remove any options that are not required
                    for (SzGrpcServerOption opt : group) {
                        if (opt.isGroupOptional())
                            others.remove(opt);
                    }

                    // make the others set unmodifiable
                    others = Collections.unmodifiableSet(others);

                    // add the dependency
                    dependencyMap.put(option, Set.of(others));
                }
            });

            CONFLICTING_OPTIONS = recursivelyUnmodifiableMap(conflictMap);
            ALTERNATIVE_OPTIONS = recursivelyUnmodifiableMap(altMap);
            OPTIONS_BY_FLAG = Collections.unmodifiableMap(lookupMap);
            DEPENDENCIES = Collections.unmodifiableMap(dependencyMap);

        } catch (Exception e) {
            e.printStackTrace();
            throw new ExceptionInInitializerError(e);
        }
    }

    /**
     * The {@link ParameterProcessor} implementation for this class.
     */
    private static class ParamProcessor implements ParameterProcessor {
        /**
         * Processes the parameters for the specified option.
         *
         * @param option The {@link SzGrpcServerOption} to process.
         * @param params The {@link List} of parameters for the option.
         * @return The processed value.
         * @throws IllegalArgumentException If the specified {@link
         *                                  CommandLineOption} is not an instance of
         *                                  {@link
         *                                  SzGrpcServerOption} or is otherwise
         *                                  unrecognized.
         */
        public Object process(CommandLineOption option, List<String> params) {
            // check if unhandled
            if (!(option instanceof SzGrpcServerOption)) {
                throw new IllegalArgumentException(
                        "Unhandled command line option: " + option.getCommandLineFlag()
                                + " / " + option);
            }

            // down-cast
            SzGrpcServerOption serverOption = (SzGrpcServerOption) option;
            switch (serverOption) {
                case HELP:
                case VERSION:
                    return Boolean.TRUE;

                case GRPC_PORT: {
                    int port = Integer.parseInt(params.get(0));
                    if (port < 0) {
                        throw new IllegalArgumentException(
                            "Negative port numbers are not allowed: " + port);
                    }
                    return port;
                }

                case BIND_ADDRESS:
                    String addrArg = params.get(0);
                    InetAddress addr = null;
                    try {
                        if ("all".equals(addrArg)) {
                            addr = InetAddress.getByName("0.0.0.0");
                        } else if ("loopback".equals(addrArg)) {
                            addr = InetAddress.getLoopbackAddress();
                        } else {
                            addr = InetAddress.getByName(addrArg);
                        }
                    } catch (RuntimeException e) {
                        throw e;
                    } catch (Exception e) {
                        throw new IllegalArgumentException(e);
                    }
                    return addr;

                case CORE_INSTANCE_NAME:
                    return params.get(0).trim();

                case CORE_SETTINGS: {
                    String paramVal = params.get(0).trim();
                    if (paramVal.length() == 0) {
                        throw new IllegalArgumentException(
                                "Missing parameter for core settings.");
                    }
                    if (paramVal.startsWith("{")) {
                        try {
                            return JsonUtilities.parseJsonObject(paramVal);

                        } catch (Exception e) {
                            throw new IllegalArgumentException(
                                    multilineFormat(
                                       "Core settings is not valid JSON: ",
                                        paramVal));
                        }
                    } else {
                        File initFile = new File(paramVal);
                        if (!initFile.exists()) {
                            throw new IllegalArgumentException(
                                    "Specified JSON init file does not exist: " + initFile);
                        }
                        String jsonText;
                        try {
                            jsonText = readTextFileAsString(initFile, "UTF-8");

                        } catch (IOException e) {
                            throw new RuntimeException(
                                    multilineFormat(
                                            "Failed to read JSON initialization file: "
                                                + initFile,
                                            "",
                                            "Cause: " + e.getMessage()));
                        }
                        try {
                            return JsonUtilities.parseJsonObject(jsonText);

                        } catch (Exception e) {
                            throw new IllegalArgumentException(
                                    "The initialization file does not contain valid JSON: "
                                            + initFile);
                        }
                    }
                }
                case CORE_CONFIG_ID:
                    try {
                        return Long.parseLong(params.get(0));
                    } catch (Exception e) {
                        throw new IllegalArgumentException(
                                "The configuration ID for " + option.getCommandLineFlag()
                                        + " must be an integer: " + params.get(0));
                    }

                case CORE_LOG_LEVEL: {
                    String paramVal = params.get(0).trim().toLowerCase();

                    switch (paramVal) {
                        case "verbose":
                        case "1":
                            return true;
                        case "muted":
                        case "0":
                            return false;
                        default:
                            throw new IllegalArgumentException(
                                "The specified core log level is not recognized; " + paramVal);
                    }
                }

                case CORE_CONCURRENCY: 
                case GRPC_CONCURRENCY: {
                    int threadCount;
                    try {
                        threadCount = Integer.parseInt(params.get(0));
                    } catch (IllegalArgumentException e) {
                        throw new IllegalArgumentException(
                                "Thread count must be an integer: " + params.get(0));
                    }
                    if (threadCount <= 0) {
                        throw new IllegalArgumentException(
                                "Negative thread counts are not allowed: " + threadCount);
                    }
                    return threadCount;
                }

                case REFRESH_CONFIG_SECONDS:
                    try {
                        return Long.parseLong(params.get(0));
                    } catch (Exception e) {
                        throw new IllegalArgumentException(
                                "The specified refresh period for "
                                        + option.getCommandLineFlag() + " must be an integer: "
                                        + params.get(0));
                    }
                
                case LOG_STATS_SECONDS: {
                    long statsInterval;
                    try {
                        statsInterval = Long.parseLong(params.get(0));
                    } catch (IllegalArgumentException e) {
                        throw new IllegalArgumentException(
                                "Stats interval must be an integer: " + params.get(0));
                    }
                    if (statsInterval < 0) {
                        throw new IllegalArgumentException(
                                "Negative stats intervals are not allowed: "
                                        + statsInterval);
                    }
                    return statsInterval;
                }

                case SKIP_STARTUP_PERF:
                case SKIP_ENGINE_PRIMING:
                case IGNORE_ENVIRONMENT:
                    if (params.size() == 0)
                        return Boolean.TRUE;
                    String boolText = params.get(0);
                    if ("false".equalsIgnoreCase(boolText)) {
                        return Boolean.FALSE;
                    }
                    if ("true".equalsIgnoreCase(boolText)) {
                        return Boolean.TRUE;
                    }
                    throw new IllegalArgumentException(
                            "The specified parameter for "
                                    + option.getCommandLineFlag()
                                    + " must be true or false: " + params.get(0));

                default:
                    throw new IllegalArgumentException(
                            "Unhandled command line option: "
                                    + option.getCommandLineFlag()
                                    + " / " + option);
            }
        }
    }

    /**
     * The {@link ParameterProcessor} for {@link SzGrpcServerOption}.
     * This instance will only handle instances of {@link CommandLineOption}
     * instances of type {@link SzGrpcServerOption}.
     */
    public static final ParameterProcessor PARAMETER_PROCESSOR 
        = new ParamProcessor();
        
    /**
     * Convenience method to parse the command line via 
     * {@link CommandLineUtilities#parseCommandLine(Class, String[], ParameterProcessor, CommandLineOption, List)}
     * and convert the result to a {@link Map} with keys typed on
     * {@link SzGrpcServerOption}.
     * 
     * @param args The arguments to parse.
     * @param deprecationWarnings The {@link List} to be populated with any
     *                            {@link DeprecatedOptionWarning} instances that
     *                            are generated, or <code>null</code> if the
     *                            caller is not interested in deprecation
     *                            warnings.
     * @param jsonBuilder If not <code>null</code> then this {@link JsonObjectBuilder}
     *                    is populated with startup option information.
     *
     * @return The {@link Map} of {@link SzGrpcServerOption} keys to 
     *         {@link Object} values representing the values for
     *         those options.
     *
     * @throws CommandLineException If a command-line option parsing error occurs.
     * 
     */
    public static Map<CommandLineOption, Object> parseCommandLine(
            String[]                        args,
            List<DeprecatedOptionWarning>   deprecationWarnings,
            JsonObjectBuilder               jsonBuilder)            
        throws CommandLineException
    {
        Map<CommandLineOption, CommandLineValue> optionValues 
            = CommandLineUtilities.parseCommandLine(
                SzGrpcServerOption.class,
                args, SzGrpcServerOption.PARAMETER_PROCESSOR,
                IGNORE_ENVIRONMENT, deprecationWarnings);

        Map<CommandLineOption, Object> processedValues
            = CommandLineUtilities.processCommandLine(
                optionValues, null, jsonBuilder, null);
        
        return processedValues;
    }
}
