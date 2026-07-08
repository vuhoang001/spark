#!/usr/bin/env bash
set -euo pipefail

echo "Setting up local Python virtual environment and dependencies..."

if command -v java >/dev/null 2>&1; then
  JAVA_VERSION=$(java -version 2>&1 | head -n 1 | awk -F '"' '{print $2}')
  JAVA_MAJOR=$(echo "$JAVA_VERSION" | awk -F. '{print $1}')
  if [ "$JAVA_MAJOR" -lt 17 ]; then
    echo "WARNING: Java 17+ is required for Spark. Current version: $JAVA_VERSION"
    echo "Install openjdk-17-jdk before running Spark jobs."
  fi
else
  echo "WARNING: Java is not installed. Install openjdk-17-jdk before running Spark jobs."
fi

python3 -m venv venv
source venv/bin/activate
pip install --upgrade pip
pip install pyspark==3.2.2 psycopg2-binary sqlalchemy confluent-kafka chispa

echo "Done. Activate with: source venv/bin/activate"
echo "If you want Docker infrastructure, run: docker compose up -d"
