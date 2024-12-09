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

public class AgentLoader {
    private static final List<String> FULL_COMMAND = new ArrayList<>();

    private static final Set<String> PLUGIN_COMMAND = new HashSet<>();

    private static final Set<String> WITH_CONFIG_COMMAND = new HashSet<>();

    private static final Map<String, String> COMMAND_DETAILS = new HashMap<>();

    private static boolean validIndexFlag = false;

    private static final int RETRY_COUNT = 3;

    private AgentLoader() {
    }

    /**
     * AgentLoader 的main方法
     */
    public static void main(String[] args)
            throws IOException, AttachNotSupportedException, AgentLoadException, AgentInitializationException {
        initCommandCollection();

        List<VirtualMachineDescriptor> vmDescriptors = VirtualMachine.list();

        if (vmDescriptors.isEmpty()) {
            System.out.println("没有找到 Java 进程");
            return;
        }

        System.out.println("请选择需要使用Sermant Agent的Java进程：");
        for (int i = 0; i < vmDescriptors.size(); i++) {
            VirtualMachineDescriptor descriptor = vmDescriptors.get(i);
            System.out.println(i + ": " + descriptor.id() + " " + descriptor.displayName());
        }

        // 读取用户输入的序号
        BufferedReader userInputReader = new BufferedReader(new InputStreamReader(System.in));
        int selectedProcessIndex = 0;
        int retryCount = RETRY_COUNT;
        while (!validIndexFlag && retryCount > 0) {
            System.out.print("请输入需要使用Sermant Agent的Java进程序号：");
            selectedProcessIndex = Integer.parseInt(userInputReader.readLine());

            if (selectedProcessIndex >= 0 && selectedProcessIndex < vmDescriptors.size()) {
                validIndexFlag = true;
            } else {
                System.out.println("无效的进程序号，请输入范围内的序号。");
                retryCount--;
            }
        }

        if (!validIndexFlag) {
            System.out.println("重试次数已用尽，操作失败。");
            return;
        }
        validIndexFlag = false;

        // 连接到选定的虚拟机
        VirtualMachineDescriptor selectedDescriptor = vmDescriptors.get(selectedProcessIndex);
        System.out.println("您选择的进程 ID 是：" + selectedDescriptor.id());

        VirtualMachine vm = VirtualMachine.attach(selectedDescriptor);

        // 获取Sermant Agent目录
        System.out.print("请输入Sermant Agent所在目录（默认采用该目录下sermant-agent.jar为入口）：");
        String agentPath = userInputReader.readLine();

        // 展示目前支持的命令列表
        System.out.println("请选择需要执行的命令：");
        for (int i = 0; i < FULL_COMMAND.size(); i++) {
            String command = FULL_COMMAND.get(i);
            System.out.println(i + ": " + command);
            System.out.println("命令说明：" + COMMAND_DETAILS.get(command));
        }

        int selectedCommandIndex = 0;
        retryCount = RETRY_COUNT;
        while (!validIndexFlag && retryCount > 0) {
            System.out.print("请输入您要执行命令的序号：");
            selectedCommandIndex = Integer.parseInt(userInputReader.readLine());

            if (selectedProcessIndex >= 0 && selectedCommandIndex < FULL_COMMAND.size()) {
                validIndexFlag = true;
            } else {
                System.out.println("无效的命令序号，请输入范围内的序号。");
                retryCount--;
            }
        }

        if (!validIndexFlag) {
            System.out.println("重试次数已用尽，操作失败。");
            return;
        }
        validIndexFlag = false;

        String currentCommand = FULL_COMMAND.get(selectedCommandIndex);

        if (PLUGIN_COMMAND.contains(currentCommand)) {
            System.out.print("请输入您要操作的插件名称，多个插件使用/分隔：");
            currentCommand += ":";
            currentCommand += userInputReader.readLine();
        }

        String agentArgs = "agentPath=" + agentPath + ",";
        if (WITH_CONFIG_COMMAND.contains(FULL_COMMAND.get(selectedCommandIndex))) {
            // 获取传入Sermant Agent的参数
            System.out.print("请输入向Sermant Agent传入的参数(可为空, 示例格式：key1=value1,key2=value2)：");
            if (currentCommand.equals("INSTALL-AGENT")) {
                agentArgs += userInputReader.readLine();
            } else {
                agentArgs += "command=" + currentCommand + "," +
                        userInputReader.readLine();
            }
            // 关闭资源
            userInputReader.close();

            // 启动Sermant Agent
            vm.loadAgent(agentPath + "/sermant-agent.jar", agentArgs);
            vm.detach();
            System.out.println("命令执行完毕，脚本已退出");
            return;
        }

        agentArgs += "command=" + currentCommand + ",";
        // 关闭资源
        userInputReader.close();

        // 启动Sermant Agent
        vm.loadAgent(agentPath + "/sermant-agent.jar", agentArgs);
        vm.detach();
        System.out.println("命令执行完毕，脚本已退出");
    }

    private static void initCommandCollection() {
        // 填充目前支持的命令
        FULL_COMMAND.add("INSTALL-AGENT");
        FULL_COMMAND.add("UNINSTALL-AGENT");
        FULL_COMMAND.add("INSTALL-PLUGINS");
        FULL_COMMAND.add("UNINSTALL-PLUGINS");
        FULL_COMMAND.add("UPDATE-PLUGINS");
        FULL_COMMAND.add("CHECK-ENHANCEMENT");

        // 命令描述
        COMMAND_DETAILS.put("INSTALL-AGENT", "安装Sermant Agent，同时安装plugins.yaml配置文件中dynamicPlugins.active下的所有插件");
        COMMAND_DETAILS.put("UNINSTALL-AGENT", "卸载Sermant Agent，同时卸载所有已安装插件");
        COMMAND_DETAILS.put("INSTALL-PLUGINS", "安装插件至Sermant Agent中，Sermant Agent未安装时会自动安装Agent（同时安装plugins"
                + ".yaml配置文件中dynamicPlugins.active下的所有插件）");
        COMMAND_DETAILS.put("UNINSTALL-PLUGINS", "卸载Sermant Agent中的插件");
        COMMAND_DETAILS.put("UPDATE-PLUGINS", "更新Sermant Agent插件");
        COMMAND_DETAILS.put("CHECK-ENHANCEMENT", "查询Sermant Agent已安装插件和相应插件对应的增强信息（包括被增强的类和方法，及对应的拦截器）");

        // 动态热插拔插件的命令
        PLUGIN_COMMAND.add("INSTALL-PLUGINS");
        PLUGIN_COMMAND.add("UNINSTALL-PLUGINS");
        PLUGIN_COMMAND.add("UPDATE-PLUGINS");

        // 需要传入Sermant Agent参数的命令
        WITH_CONFIG_COMMAND.add("INSTALL-AGENT");
        WITH_CONFIG_COMMAND.add("INSTALL-PLUGINS");
        WITH_CONFIG_COMMAND.add("UPDATE-PLUGINS");
    }
}
