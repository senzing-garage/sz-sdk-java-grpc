package com.senzing.sdk.grpc.server;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static com.senzing.sdk.grpc.server.InstallUtilities.*;

/**
 * Provides a wrapper main function entry point that ensures the <code>sz-sdk.jar</code>
 * is on the class path and if not, launches a sub-process to handle it.  
 */
final class WrapperMain {
    /**
     * The package name for this class.
     */
    static final String PACKAGE_NAME = WrapperMain.class.getPackageName();

    /**
     * The fully-qualified class name of the SzGrpcServer class.
     */
    static final String SERVER_CLASS_NAME = PACKAGE_NAME + ".SzGrpcServer";

    /**
     * The fully-qualified class name of an SDK-specific class.
     */
    static final String SDK_CLASS_NAME = "com.senzing.sdk.core.SzCoreEnvironment";

    /**
     * Private default constructor.
     */
    private WrapperMain() {
        // do nothing
    }

    /**
     * Wrapper main to ensure <code>sz-sdk.jar</code> is in the path.
     * 
     * @param args The command-line arguments.
     */
    public static void main(String[] args) {
        boolean foundSenzingSdk = false;
        try {
            Class.forName(SDK_CLASS_NAME);
            foundSenzingSdk = true;

        } catch (ClassNotFoundException e) {
            foundSenzingSdk = false;
        }

        // check if the senzing SDK was found
        if (foundSenzingSdk) {
            try {
                Class<?> serverClass = Class.forName(SERVER_CLASS_NAME);
                
                Method mainMethod = serverClass.getMethod(
                    "main", String[].class);

                mainMethod.invoke(null, ((Object) args));

            } catch (ClassNotFoundException|NoSuchMethodException|InvocationTargetException|IllegalAccessException e) {
                System.err.println("Failed to execute " + SERVER_CLASS_NAME 
                                   + ".main(): " + e.getMessage());
                System.exit(1);
            }

        } else {
            // check if we have no install jar file
            if (INSTALL_JAR_FILE == null) {
                System.err.println();
                System.err.println(
                    "The Senzing native library " + LIBRARY_FILE_NAME 
                    + " could not be found in the library");
                System.err.println(
                    "path defined by the " + LIBRARY_PATH_ENV_VARIABLE 
                    + " environment variable.");
                System.err.println();
                System.err.println(
                    "Please check your " + LIBRARY_PATH_ENV_VARIABLE + " environment setting and "
                    + "try again.");
                
                if (RUNTIME_OS_TYPE == OSType.MACOS) {
                    System.err.println();
                    System.err.println(
                        "NOTE: System Integrity Protection (SIP) on macOS will prevent the Java process from");
                    System.err.println(
                        "inheriting the " + LIBRARY_PATH_ENV_VARIABLE + " environment variable if using a Java executable");
                    System.err.println(
                        "from a \"system path\" (including the default /usr/bin/java wrapper which finds Java");
                    System.err.println(
                        "via JAVA_HOME).");
                    System.err.println();
                    System.err.println(
                        "You are required to directly execute a Java executable from a \"user path\" on macOS.");
                    System.err.println(
                        "For example, install Java in your home directory and execute it from there.");
                    System.err.println();
                }

                System.exit(1);
            }

            // get the java executable
            String java = ProcessHandle.current().info().command().orElse("java");

            // create the class path
            String mainJarPath  = findJarForClass(WrapperMain.class).getPath();
            String sdkJarPath   = INSTALL_JAR_FILE.toString();
            String pathSep      = System.getProperty("path.separator");
            String classPath    = mainJarPath + pathSep + sdkJarPath;
            
            // build the command line list
            List<String> commandLine = new ArrayList<>(args.length + 4);
            commandLine.add(java);
            commandLine.add("-cp");
            commandLine.add(classPath);
            commandLine.add(SERVER_CLASS_NAME);
            for (String arg : args) {
                commandLine.add(arg);
            }

            ProcessBuilder pb = new ProcessBuilder(commandLine);
            try { 
                pb.inheritIO();

                Process process = pb.start();
                ProcessHandle handle = process.toHandle();

                // add a shutdown hook for signal forwarding
                Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                    if (handle.isAlive()) {
                        // first try graceful shutdown (SIGTERM)
                        handle.destroy();
                        try {
                            // wait up to 30 seconds for graceful shutdown
                            if (!handle.onExit().get(30, TimeUnit.SECONDS).isAlive()) 
                            {
                                return; // exit gracefully
                            }
                        } catch (TimeoutException e) {
                            System.err.println(
                                "Process did not exit gracefully, forcing shutdown...");
                        } catch (Exception e) {
                            // Interrupted or execution exception
                        }

                        // force-kill if still alive
                        if (handle.isAlive()) {
                            handle.destroyForcibly();
                        }
                    }
                }));

                System.exit(process.waitFor());

            } catch (Exception e) {
                System.err.println("Failed to launch server process: " + e.getMessage());
                System.exit(1);
            }
        }
    }
}
