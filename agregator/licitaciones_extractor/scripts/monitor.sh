#!/bin/bash

# Licitaciones Extractor - Production Monitoring Script
# Usage: ./scripts/monitor.sh [options]

set -e

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

CONTAINER_NAME="licitaciones_extractor_prod"
COMPOSE_FILE="docker-compose.prod.yml"

# Function to print colored output
print_status() {
    echo -e "${BLUE}[INFO]${NC} $1"
}

print_success() {
    echo -e "${GREEN}[SUCCESS]${NC} $1"
}

print_warning() {
    echo -e "${YELLOW}[WARNING]${NC} $1"
}

print_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

# Function to show usage
show_usage() {
    echo "Usage: $0 [command]"
    echo ""
    echo "Commands:"
    echo "  status          Show container status and health"
    echo "  logs            Show recent application logs"
    echo "  stats           Show resource usage statistics"
    echo "  test            Run system tests"
    echo "  metrics         Show extraction metrics"
    echo "  quality         Show data quality report"
    echo "  restart         Restart the application"
    echo "  backup          Create database backup"
    echo "  cleanup         Cleanup old logs and data"
    echo ""
    echo "Examples:"
    echo "  $0 status       # Check system status"
    echo "  $0 logs         # View recent logs"
    echo "  $0 metrics      # Show extraction metrics"
}

# Function to check if container is running
check_container() {
    if ! docker ps | grep -q "$CONTAINER_NAME"; then
        print_error "Container $CONTAINER_NAME is not running"
        return 1
    fi
    return 0
}

# Function to show container status
show_status() {
    print_status "Checking container status..."

    if docker-compose -f "$COMPOSE_FILE" ps | grep -q "Up"; then
        print_success "Containers are running"
        docker-compose -f "$COMPOSE_FILE" ps

        print_status "Container health status:"
        docker inspect "$CONTAINER_NAME" --format='{{.State.Health.Status}}' 2>/dev/null || echo "No health check defined"

        print_status "Uptime:"
        docker inspect "$CONTAINER_NAME" --format='Started: {{.State.StartedAt}}' 2>/dev/null || echo "Cannot get uptime"
    else
        print_error "Containers are not running"
        docker-compose -f "$COMPOSE_FILE" ps
        return 1
    fi
}

# Function to show logs
show_logs() {
    print_status "Recent application logs (last 50 lines):"
    echo ""

    if check_container; then
        docker-compose -f "$COMPOSE_FILE" logs --tail=50 licitaciones-extractor
    else
        print_error "Cannot retrieve logs - container not running"
        return 1
    fi
}

# Function to show statistics
show_stats() {
    print_status "Resource usage statistics:"
    echo ""

    if check_container; then
        docker stats "$CONTAINER_NAME" --no-stream --format "table {{.Name}}\t{{.CPUPerc}}\t{{.MemUsage}}\t{{.MemPerc}}\t{{.NetIO}}\t{{.BlockIO}}"
    else
        print_error "Cannot retrieve stats - container not running"
        return 1
    fi
}

# Function to run tests
run_tests() {
    print_status "Running system tests..."

    if check_container; then
        if docker exec "$CONTAINER_NAME" python src/main.py --mode=test; then
            print_success "All tests passed"
        else
            print_error "Some tests failed"
            return 1
        fi
    else
        return 1
    fi
}

# Function to show metrics
show_metrics() {
    print_status "Extraction metrics:"
    echo ""

    if check_container; then
        docker exec "$CONTAINER_NAME" python src/main.py --mode=monitor
    else
        print_error "Cannot retrieve metrics - container not running"
        return 1
    fi
}

# Function to show quality report
show_quality() {
    print_status "Data quality report:"
    echo ""

    if check_container; then
        docker exec "$CONTAINER_NAME" python src/main.py --mode=quality-report
    else
        print_error "Cannot retrieve quality report - container not running"
        return 1
    fi
}

# Function to restart application
restart_app() {
    print_status "Restarting application..."

    docker-compose -f "$COMPOSE_FILE" restart licitaciones-extractor

    sleep 5

    if show_status; then
        print_success "Application restarted successfully"
    else
        print_error "Failed to restart application"
        return 1
    fi
}

# Function to backup database
backup_database() {
    print_status "Creating database backup..."

    BACKUP_DIR="./backups"
    mkdir -p "$BACKUP_DIR"

    DATE=$(date +%Y%m%d_%H%M%S)
    BACKUP_FILE="$BACKUP_DIR/licitaciones_backup_$DATE.sql"

    if check_container; then
        # Get database URL from container
        DB_URL=$(docker exec "$CONTAINER_NAME" env | grep POSTGRES_URL | cut -d= -f2-)

        if [[ -n "$DB_URL" ]]; then
            print_status "Backing up database to $BACKUP_FILE"

            # Use docker exec to run pg_dump inside container
            if docker exec "$CONTAINER_NAME" sh -c "pg_dump '$DB_URL' > /tmp/backup.sql && cat /tmp/backup.sql" > "$BACKUP_FILE"; then
                print_success "Database backup created: $BACKUP_FILE"

                # Compress the backup
                gzip "$BACKUP_FILE"
                print_success "Backup compressed: $BACKUP_FILE.gz"
            else
                print_error "Failed to create database backup"
                return 1
            fi
        else
            print_error "Could not find database URL"
            return 1
        fi
    else
        return 1
    fi
}

# Function to cleanup old files
cleanup_old_data() {
    print_status "Cleaning up old files..."

    # Cleanup old logs (older than 30 days)
    find ./logs -name "*.log*" -mtime +30 -delete 2>/dev/null || true
    print_status "Cleaned up old log files"

    # Cleanup old backups (older than 90 days)
    find ./backups -name "*.sql.gz" -mtime +90 -delete 2>/dev/null || true
    print_status "Cleaned up old backup files"

    # Docker cleanup
    docker system prune -f > /dev/null 2>&1 || true
    print_status "Cleaned up Docker resources"

    print_success "Cleanup completed"
}

# Main script logic
case "${1:-status}" in
    status)
        show_status
        ;;
    logs)
        show_logs
        ;;
    stats)
        show_stats
        ;;
    test)
        run_tests
        ;;
    metrics)
        show_metrics
        ;;
    quality)
        show_quality
        ;;
    restart)
        restart_app
        ;;
    backup)
        backup_database
        ;;
    cleanup)
        cleanup_old_data
        ;;
    help|--help|-h)
        show_usage
        ;;
    *)
        print_error "Unknown command: $1"
        show_usage
        exit 1
        ;;
esac