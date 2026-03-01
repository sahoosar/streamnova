#!/usr/bin/env bash
# Use Java 21 in this terminal for the StreamNova project.
# Run:  source use-java21.sh   (or  . use-java21.sh)
# Then: java -version   and   mvn compile

if [[ -d "$HOME/.sdkman/candidates/java/21-tem" ]]; then
  export JAVA_HOME="$HOME/.sdkman/candidates/java/21-tem"
elif [[ -d "$HOME/.sdkman/candidates/java/21.0"* ]]; then
  export JAVA_HOME=$(ls -d "$HOME/.sdkman/candidates/java/21.0"* 2>/dev/null | head -1)
elif [[ -d "/Library/Java/JavaVirtualMachines/temurin-21.jdk/Contents/Home" ]]; then
  export JAVA_HOME="/Library/Java/JavaVirtualMachines/temurin-21.jdk/Contents/Home"
elif [[ -d "/Library/Java/JavaVirtualMachines/openjdk-21.jdk/Contents/Home" ]]; then
  export JAVA_HOME="/Library/Java/JavaVirtualMachines/openjdk-21.jdk/Contents/Home"
else
  echo "JDK 21 not found. Install with: sdk install java 21-tem"
  echo "Or download from: https://adoptium.net/temurin/releases/?version=21"
  return 1 2>/dev/null || exit 1
fi
export PATH="$JAVA_HOME/bin:$PATH"
echo "Using JAVA_HOME=$JAVA_HOME"
java -version
