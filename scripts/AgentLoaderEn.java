/*
 * Copyright (C) 2024-2024 Huawei Technologies Co., Ltd. All rights reserved.
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

import com.sun.tools.attach.AgentInitializationException;
import com.sun.tools.attach.AgentLoadException;
import com.sun.tools.attach.AttachNotSupportedException;
import com.sun.tools.attach.VirtualMachine;
import com.sun.tools.attach.VirtualMachineDescriptor;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class AgentLoaderEn {
    private static final List<String> FULL_COMMAND = new ArrayList<>();

    private static final Set<String> PLUGIN_COMMAND = new HashSet<>();

    private static final Set<String> WITH_CONFIG_COMMAND = new HashSet<>();

    private static final Map<String, String> COMMAND_DETAILS = new HashMap<>();

    private static boolean validIndexFlag = false;

    private static final int RETRY_COUNT = 3;

    private AgentLoaderEn() {
    }

    /**
     * AgentLoader main method
     */
    public static void main(String[] args)
            throws IOException, AttachNotSupportedException, AgentLoadException, AgentInitializationException {
        initCommandCollection();

        List<VirtualMachineDescriptor> vmDescriptors = VirtualMachine.list();

        if (vmDescriptors.isEmpty()) {
            System.out.println("not find Java process");
            return;
        }

        System.out.println("Please select the Java process you wish to use with Sermant Agent: ");
        for (int i = 0; i < vmDescriptors.size(); i++) {
            VirtualMachineDescriptor descriptor = vmDescriptors.get(i);
            System.out.println(i + ": " + descriptor.id() + " " + descriptor.displayName());
        }

        // read the user-inputted number
        BufferedReader userInputReader = new BufferedReader(new InputStreamReader(System.in));
        int selectedProcessIndex = 0;
        int retryCount = RETRY_COUNT;
        while (!validIndexFlag && retryCount > 0) {
            System.out.print("Please enter the Java process number you wish to use with Sermant Agent: ");
            selectedProcessIndex = Integer.parseInt(userInputReader.readLine());

            if (selectedProcessIndex >= 0 && selectedProcessIndex < vmDescriptors.size()) {
                validIndexFlag = true;
            } else {
                System.out.println("Invalid process number, please enter a number within the valid range.");
                retryCount--;
            }
        }

        if (!validIndexFlag) {
            System.out.println("Retry attempts exhausted, operation failed.");
            return;
        }
        validIndexFlag = false;

        // connect to the selected virtual machine
        VirtualMachineDescriptor selectedDescriptor = vmDescriptors.get(selectedProcessIndex);
        System.out.println("The process ID you selected: " + selectedDescriptor.id());

        VirtualMachine vm = VirtualMachine.attach(selectedDescriptor);

        // get the Sermant Agent directory
        System.out.print("Please enter the directory where Sermant Agent is located (by default, it uses sermant-agent.jar in this directory as the entry point): ");
        String agentPath = userInputReader.readLine();

        // display the list of currently supported commands
        System.out.println("Please select the command to execute: ");
        for (int i = 0; i < FULL_COMMAND.size(); i++) {
            String command = FULL_COMMAND.get(i);
            System.out.println(i + ": " + command);
            System.out.println("Command description: " + COMMAND_DETAILS.get(command));
        }

        int selectedCommandIndex = 0;
        retryCount = RETRY_COUNT;
        while (!validIndexFlag && retryCount > 0) {
            System.out.print("Please enter the number of the command you want to execute: ");
            selectedCommandIndex = Integer.parseInt(userInputReader.readLine());

            if (selectedProcessIndex >= 0 && selectedCommandIndex < FULL_COMMAND.size()) {
                validIndexFlag = true;
            } else {
                System.out.println("Invalid command number, please enter a number within the valid range.");
                retryCount--;
            }
        }

        if (!validIndexFlag) {
            System.out.println("Retry attempts exhausted, operation failed.");
            return;
        }
        validIndexFlag = false;

        String currentCommand = FULL_COMMAND.get(selectedCommandIndex);

        if (PLUGIN_COMMAND.contains(currentCommand)) {
            System.out.print("Please enter the name of the plugin you want to operate on, "
                    + "separated by / for multiple plugins: ");
            currentCommand += ":";
            currentCommand += userInputReader.readLine();
        }

        String agentArgs = "agentPath=" + agentPath + ",";
        if (WITH_CONFIG_COMMAND.contains(FULL_COMMAND.get(selectedCommandIndex))) {
            // Get the parameters passed to the Sermant Agent
            System.out.print("Enter the parameters to pass to the Sermant Agent (optional, example format: "
                    + "key1=value1,key2=value2): ");
            if (currentCommand.equals("INSTALL-AGENT")) {
                agentArgs += userInputReader.readLine();
            } else {
                agentArgs += "command=" + currentCommand + "," +
                        userInputReader.readLine();
            }
            // close resource
            userInputReader.close();

            // start Sermant Agent
            vm.loadAgent(agentPath + "/sermant-agent.jar", agentArgs);
            vm.detach();
            System.out.println("Command execution completed, the script has exited.");
            return;
        }

        agentArgs += "command=" + currentCommand + ",";
        // close resource
        userInputReader.close();

        // start Sermant Agent
        vm.loadAgent(agentPath + "/sermant-agent.jar", agentArgs);
        vm.detach();
        System.out.println("Command execution completed, the script has exited.");
    }

    private static void initCommandCollection() {
        // add the currently supported commands.
        FULL_COMMAND.add("INSTALL-AGENT");
        FULL_COMMAND.add("UNINSTALL-AGENT");
        FULL_COMMAND.add("INSTALL-PLUGINS");
        FULL_COMMAND.add("UNINSTALL-PLUGINS");
        FULL_COMMAND.add("UPDATE-PLUGINS");
        FULL_COMMAND.add("CHECK-ENHANCEMENT");

        // command description
        COMMAND_DETAILS.put("INSTALL-AGENT", "Install the Sermant Agent and all plugins listed under "
                + "dynamicPlugins.active in the plugins.yaml configuration file.");
        COMMAND_DETAILS.put("UNINSTALL-AGENT", "Uninstall the Sermant Agent along with all installed plugins.");
        COMMAND_DETAILS.put("INSTALL-PLUGINS", "Install plugins into the Sermant Agent. "
                + "If the Sermant Agent is not installed, it will be automatically installed (along with all plugins "
                + "listed under dynamicPlugins.active in the plugins.yaml configuration file).");
        COMMAND_DETAILS.put("UNINSTALL-PLUGINS", "Uninstall plugins from the Sermant Agent.");
        COMMAND_DETAILS.put("UPDATE-PLUGINS", "Update plugins from the Sermant Agent.");
        COMMAND_DETAILS.put("CHECK-ENHANCEMENT", "Query the plugins installed in the Sermant Agent and the "
                + "corresponding enhancement information (including enhanced classes and methods, "
                + "as well as the corresponding interceptors).");

        // command for dynamically hot-plugging plugins.
        PLUGIN_COMMAND.add("INSTALL-PLUGINS");
        PLUGIN_COMMAND.add("UNINSTALL-PLUGINS");
        PLUGIN_COMMAND.add("UPDATE-PLUGINS");

        // commands that require parameters to be passed to the Sermant Agent.
        WITH_CONFIG_COMMAND.add("INSTALL-AGENT");
        WITH_CONFIG_COMMAND.add("INSTALL-PLUGINS");
        WITH_CONFIG_COMMAND.add("UPDATE-PLUGINS");
    }
}
