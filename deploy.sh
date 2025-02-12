#!/bin/bash

# Get absolute path to script directory
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
PROJ_DIR="${SCRIPT_DIR}/StarterCode_PA1"
PYTHON="python3"
VENV_NAME="venv"
VENV_PATH="${SCRIPT_DIR}/${VENV_NAME}"

# Check if project directory exists
if [ ! -d "$PROJ_DIR" ]; then
    echo "Error: Project directory $PROJ_DIR not found"
    exit 1
fi

# Setup Python virtual environment
setup_venv() {
    echo "Setting up Python virtual environment..."
    if [ ! -d "$VENV_PATH" ]; then
        python3 -m venv $VENV_PATH
    fi
    source $VENV_PATH/bin/activate
}

# Check dependencies
check_dependencies() {
    echo "Checking dependencies..."
    setup_venv
    pip3 install -q zmq protobuf
}

# Function to start Discovery service
start_discovery() {
    echo "Starting Discovery Service..."
    source $VENV_PATH/bin/activate
    $PYTHON ${PROJ_DIR}/DiscoveryAppln.py "$@" > ${SCRIPT_DIR}/discovery.log 2>&1 &
    echo "Discovery Service started. Check discovery.log for output."
}

# Function to start Publisher
start_publisher() {
    echo "Starting Publisher..."
    source $VENV_PATH/bin/activate
    $PYTHON ${PROJ_DIR}/PublisherAppln.py "$@" > "${SCRIPT_DIR}/pub_${2}.log" 2>&1 &
    echo "Publisher started. Check pub_${2}.log for output."
}

# Function to start Subscriber
start_subscriber() {
    echo "Starting Subscriber..."
    source $VENV_PATH/bin/activate
    $PYTHON ${PROJ_DIR}/SubscriberAppln.py "$@" > "${SCRIPT_DIR}/sub_${2}.log" 2>&1 &
    echo "Subscriber started. Check sub_${2}.log for output."
}

# Function to start Broker
start_broker() {
    echo "Starting Broker..."
    source $VENV_PATH/bin/activate
    $PYTHON ${PROJ_DIR}/BrokerAppln.py "$@" > "${SCRIPT_DIR}/broker_${2}.log" 2>&1 &
    echo "Broker started. Check broker_${2}.log for output."
}

# Main execution
if [ $# -eq 0 ]; then
    echo "Usage: $0 <role> [args]"
    echo "Roles: discovery, publisher, subscriber, broker"
    exit 1
fi

# Get the role and remove it from arguments
ROLE=$1
shift

# Check dependencies first
check_dependencies

# Start the appropriate service based on role
case $ROLE in
    "discovery")
        start_discovery "$@"
        ;;
    "publisher")
        start_publisher "$@"
        ;;
    "subscriber")
        start_subscriber "$@"
        ;;
    "broker")
        start_broker "$@"
        ;;
    *)
        echo "Unknown role: $ROLE"
        echo "Valid roles are: discovery, publisher, subscriber, broker"
        exit 1
        ;;
esac

exit 0