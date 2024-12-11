This readme document is used to describe the functions of each script:

1. AesUtil.class : generate encryption keys and password ciphertexts

Run the script using the following command and enter your password as prompted:

**********************************************************************
java AesUtil
please input your password
123456
encryption key is T4bUktLn5P01Qs6unSuG5ZZElN05WUDAXOjaJgMB5eM=
encrypted password is u/K+lx/m9w1EpEjkM9R48s8PiVDHEpCUGz+1jWOasyzRrQ==
**********************************************************************

For detailed usage, please refer to the official documentation at https://sermant.io/zh/document/faq/encryption.html.

2. AgentLoader.class : sermant agent hot plugging script of java

Run the script using the following command and entering as prompted:

**********************************************************************
# Linux、MacOS
java -cp ./:$JAVA_HOME/lib/tools.jar AgentLoader

# Windows
java -cp "%JAVA_HOME%\lib\tools.jar" AgentLoader
**********************************************************************

For detailed usage, please refer to the official documentation at https://sermant.io/zh/document/user-guide/sermant-agent.html.

3. attach_sermant_agent : sermant agent hot plugging script of c language

Run the script using the following command and entering as prompted:

**********************************************************************
./attach_sermant_agent -path={sermant-path}/sermant-agent.jar -pid={pid} -command={COMMAND}
**********************************************************************

For detailed usage, please refer to the official documentation at https://sermant.io/zh/document/user-guide/sermant-agent.html#%E4%B8%80%E9%94%AE%E6%8C%82%E8%BD%BDagent%E5%92%8C%E6%8F%92%E4%BB%B6.