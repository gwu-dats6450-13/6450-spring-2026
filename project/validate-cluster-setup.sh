#!/bin/bash
# validate-cluster-setup.sh
# Automated validation of Spark cluster setup
# 
# Usage:
#   ./validate-cluster-setup.sh
#
# This replaces manual verification steps from SPARK_CLUSTER_SETUP.md
# Checks: connectivity, Java, Spark, SSH keys, worker registration, etc.

set -e

GREEN='\033[0;32m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Config
CONFIG_FILE="cluster-config.txt"
IPS_FILE="cluster-ips.txt"
CHECKS_PASSED=0
CHECKS_FAILED=0

# Helper functions
check_pass() {
    echo -e "${GREEN}✓${NC} $1"
    ((CHECKS_PASSED++))
}

check_fail() {
    echo -e "${RED}✗${NC} $1"
    ((CHECKS_FAILED++))
}

check_info() {
    echo -e "${BLUE}ℹ${NC} $1"
}

# ============================================================================
# PRE-FLIGHT CHECKS
# ============================================================================

echo -e "${YELLOW}======================================${NC}"
echo -e "${YELLOW}Spark Cluster Validation${NC}"
echo -e "${YELLOW}======================================${NC}"
echo ""

# Check configuration files exist
if [[ ! -f "$CONFIG_FILE" ]]; then
    check_fail "Configuration file not found: $CONFIG_FILE"
    check_fail "Run setup-spark-cluster.sh first to generate config"
    exit 1
fi

if [[ ! -f "$IPS_FILE" ]]; then
    check_fail "IP configuration file not found: $IPS_FILE"
    exit 1
fi

# Source configuration
source "$CONFIG_FILE"
source "$IPS_FILE"

check_pass "Configuration files loaded"

# ============================================================================
# 1. AWS CONNECTIVITY & RESOURCE CHECKS
# ============================================================================

echo ""
echo -e "${YELLOW}[1/6] AWS Connectivity${NC}"

# Check AWS CLI
if command -v aws &> /dev/null; then
    check_pass "AWS CLI installed"
else
    check_fail "AWS CLI not found"
    exit 1
fi

# Check AWS credentials
if aws sts get-caller-identity &> /dev/null; then
    ACCOUNT_ID=$(aws sts get-caller-identity --query Account --output text)
    check_pass "AWS credentials configured (Account: $ACCOUNT_ID)"
else
    check_fail "AWS credentials not configured"
    exit 1
fi

# Check EC2 instances are running
check_info "Checking EC2 instances..."
for instance in $MASTER_INSTANCE_ID $WORKER1_INSTANCE_ID $WORKER2_INSTANCE_ID; do
    status=$(aws ec2 describe-instances --instance-ids $instance --query 'Reservations[0].Instances[0].State.Name' --output text --region us-east-1)
    if [[ "$status" == "running" ]]; then
        check_pass "Instance $instance is running"
    else
        check_fail "Instance $instance is not running (State: $status)"
    fi
done

# ============================================================================
# 2. NETWORK CONNECTIVITY
# ============================================================================

echo ""
echo -e "${YELLOW}[2/6] Network Connectivity${NC}"

# Test SSH to master
if ssh -i "$KEY_FILE" -o StrictHostKeyChecking=no -o ConnectTimeout=5 ubuntu@$MASTER_PUBLIC_IP "echo 'SSH OK'" &> /dev/null; then
    check_pass "SSH connectivity to master: $MASTER_PUBLIC_IP"
else
    check_fail "SSH connectivity to master: $MASTER_PUBLIC_IP"
fi

# Test security group allows required ports
check_info "Checking security group rules..."
sg_info=$(aws ec2 describe-security-groups --group-ids $SPARK_SG_ID --query 'SecurityGroups[0].IpPermissions' --output json --region us-east-1)

# Check for key ports
for port in 22 7077 8080 4040 6066; do
    if echo "$sg_info" | grep -q "\"FromPort\": $port"; then
        check_pass "Security group allows port $port"
    else
        check_fail "Security group missing port $port rule"
    fi
done

# ============================================================================
# 3. SOFTWARE INSTALLATION CHECKS
# ============================================================================

echo ""
echo -e "${YELLOW}[3/6] Software Installation${NC}"

check_info "Checking master node..."
if ssh -i "$KEY_FILE" -o StrictHostKeyChecking=no ubuntu@$MASTER_PUBLIC_IP "java -version 2>&1 | grep -q 'openjdk'" &> /dev/null; then
    check_pass "Master: Java installed"
else
    check_fail "Master: Java not found"
fi

if ssh -i "$KEY_FILE" -o StrictHostKeyChecking=no ubuntu@$MASTER_PUBLIC_IP "ls $SPARK_HOME/bin/spark-shell &>/dev/null" &> /dev/null; then
    check_pass "Master: Spark installed"
else
    check_fail "Master: Spark not found"
fi

if ssh -i "$KEY_FILE" -o StrictHostKeyChecking=no ubuntu@$MASTER_PUBLIC_IP "python3 -c 'import pyspark' 2>/dev/null" &> /dev/null; then
    check_pass "Master: PySpark installed"
else
    check_fail "Master: PySpark not found"
fi

# Check workers
for worker_num in 1 2; do
    worker_var="WORKER${worker_num}_PUBLIC_IP"
    worker_ip="${!worker_var}"
    
    check_info "Checking worker $worker_num ($worker_ip)..."
    
    if ssh -i "$KEY_FILE" -o StrictHostKeyChecking=no -o ConnectTimeout=5 ubuntu@$worker_ip "java -version 2>&1 | grep -q 'openjdk'" &> /dev/null; then
        check_pass "Worker$worker_num: Java installed"
    else
        check_fail "Worker$worker_num: Java not found"
    fi
done

# ============================================================================
# 4. SPARK CLUSTER STATUS
# ============================================================================

echo ""
echo -e "${YELLOW}[4/6] Spark Cluster Status${NC}"

# Check master process
if ssh -i "$KEY_FILE" -o StrictHostKeyChecking=no ubuntu@$MASTER_PUBLIC_IP "jps | grep -q Master" &> /dev/null; then
    check_pass "Master process running"
else
    check_fail "Master process not running (try: \$SPARK_HOME/sbin/start-master.sh)"
fi

# Check worker processes
for worker_num in 1 2; do
    worker_var="WORKER${worker_num}_PUBLIC_IP"
    worker_ip="${!worker_var}"
    
    if ssh -i "$KEY_FILE" -o StrictHostKeyChecking=no ubuntu@$worker_ip "jps | grep -q Worker" &> /dev/null; then
        check_pass "Worker$worker_num process running"
    else
        check_fail "Worker$worker_num process not running"
    fi
done

# Check master-worker connectivity on port 7077
check_info "Testing master-worker communication on port 7077..."
for worker_num in 1 2; do
    worker_var="WORKER${worker_num}_PRIVATE_IP"
    worker_ip="${!worker_var}"
    
    # From master, try to connect to worker Spark port
    if ssh -i "$KEY_FILE" -o StrictHostKeyChecking=no ubuntu@$MASTER_PUBLIC_IP \
        "timeout 2 bash -c \"echo >/dev/tcp/$worker_ip/8081\" 2>/dev/null" &> /dev/null; then
        check_pass "Master can reach Worker$worker_num on port 8081"
    else
        check_fail "Master cannot reach Worker$worker_num on port 8081"
    fi
done

# ============================================================================
# 5. SSH KEY AUTHENTICATION
# ============================================================================

echo ""
echo -e "${YELLOW}[5/6] SSH Key Setup${NC}"

if [[ -f "$KEY_FILE" ]]; then
    check_pass "SSH private key exists: $KEY_FILE"
    
    perms=$(stat -f '%A' "$KEY_FILE" 2>/dev/null || stat -c '%a' "$KEY_FILE")
    if [[ "$perms" == "400" ]]; then
        check_pass "SSH key permissions correct (400)"
    else
        check_fail "SSH key permissions incorrect (expected 400, got $perms)"
    fi
else
    check_fail "SSH private key not found: $KEY_FILE"
fi

# Check master has SSH key for workers
if ssh -i "$KEY_FILE" -o StrictHostKeyChecking=no ubuntu@$MASTER_PUBLIC_IP "test -f ~/.ssh/id_rsa" &> /dev/null; then
    check_pass "Master has passwordless SSH key"
else
    check_fail "Master missing passwordless SSH key"
fi

# ============================================================================
# 6. DETAILED CLUSTER METRICS
# ============================================================================

echo ""
echo -e "${YELLOW}[6/6] Cluster Metrics${NC}"

# Get master memory and CPU
master_info=$(ssh -i "$KEY_FILE" -o StrictHostKeyChecking=no ubuntu@$MASTER_PUBLIC_IP \
    "echo \"CPUs: \$(nproc) | RAM: \$(free -h | grep Mem | awk '{print \$2}')\"")
check_info "Master resources: $master_info"

# Count total cores across cluster
total_cores=0
for worker_num in 1 2; do
    worker_var="WORKER${worker_num}_PUBLIC_IP"
    worker_ip="${!worker_var}"
    cores=$(ssh -i "$KEY_FILE" -o StrictHostKeyChecking=no ubuntu@$worker_ip "nproc" 2>/dev/null || echo "0")
    total_cores=$((total_cores + cores))
done
check_info "Total worker cores available: $total_cores"

# Estimate cluster processing power
check_info "Spark parallelism: ~$total_cores tasks can run simultaneously"

# ============================================================================
# SUMMARY
# ============================================================================

echo ""
echo -e "${YELLOW}======================================${NC}"
echo -e "${YELLOW}Validation Summary${NC}"
echo -e "${YELLOW}======================================${NC}"
echo ""
echo -e "Checks passed: ${GREEN}$CHECKS_PASSED${NC}"
echo -e "Checks failed: ${RED}$CHECKS_FAILED${NC}"
echo ""

if [[ $CHECKS_FAILED -eq 0 ]]; then
    echo -e "${GREEN}✓ Cluster is ready!${NC}"
    echo ""
    echo "Next steps:"
    echo "1. Access Spark Master UI: http://$MASTER_PUBLIC_IP:8080"
    echo "2. Submit a job:"
    echo "   spark-submit --master spark://$MASTER_PRIVATE_IP:7077 script.py"
    echo "3. Monitor at: http://$MASTER_PUBLIC_IP:4040"
    exit 0
else
    echo -e "${RED}✗ Cluster validation failed (see errors above)${NC}"
    echo ""
    echo "Troubleshooting:"
    echo "1. Check TROUBLESHOOTING.md for common issues"
    echo "2. SSH to master and check logs:"
    echo "   tail -50 \$SPARK_HOME/logs/spark-ubuntu-org.apache.spark.deploy.master.*.log"
    echo "3. For worker issues:"
    echo "   \$SPARK_HOME/sbin/stop-all.sh"
    echo "   \$SPARK_HOME/sbin/start-all.sh"
    exit 1
fi
