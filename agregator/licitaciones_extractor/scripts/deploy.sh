#!/bin/bash

# Licitaciones Extractor - Production Deployment Script
# Usage: ./scripts/deploy.sh [options]

set -e  # Exit on any error

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Default values
ENVIRONMENT="production"
WITH_DASHBOARD=false
WITH_REDIS=false
DETACHED=true

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
    echo "Usage: $0 [options]"
    echo ""
    echo "Options:"
    echo "  -e, --environment ENV    Environment (production|staging) [default: production]"
    echo "  -d, --dashboard         Enable dashboard service"
    echo "  -r, --redis             Enable Redis service"
    echo "  -f, --foreground        Run in foreground (don't detach)"
    echo "  -h, --help              Show this help message"
    echo ""
    echo "Examples:"
    echo "  $0                      # Basic production deployment"
    echo "  $0 -d                   # With dashboard"
    echo "  $0 -d -r               # With dashboard and Redis"
    echo "  $0 -f                   # Run in foreground"
}

# Parse command line arguments
while [[ $# -gt 0 ]]; do
    case $1 in
        -e|--environment)
            ENVIRONMENT="$2"
            shift 2
            ;;
        -d|--dashboard)
            WITH_DASHBOARD=true
            shift
            ;;
        -r|--redis)
            WITH_REDIS=true
            shift
            ;;
        -f|--foreground)
            DETACHED=false
            shift
            ;;
        -h|--help)
            show_usage
            exit 0
            ;;
        *)
            print_error "Unknown option: $1"
            show_usage
            exit 1
            ;;
    esac
done

print_status "Starting Licitaciones Extractor deployment..."
echo "Environment: $ENVIRONMENT"
echo "Dashboard: $WITH_DASHBOARD"
echo "Redis: $WITH_REDIS"
echo "Detached: $DETACHED"
echo ""

# Check prerequisites
print_status "Checking prerequisites..."

if ! command -v docker &> /dev/null; then
    print_error "Docker is not installed"
    exit 1
fi

if ! command -v docker-compose &> /dev/null; then
    print_error "Docker Compose is not installed"
    exit 1
fi

# Check if .env file exists
if [[ ! -f .env ]]; then
    print_warning ".env file not found"
    if [[ -f .env.production ]]; then
        print_status "Copying .env.production to .env"
        cp .env.production .env
        print_warning "Please edit .env file with your actual credentials"
        read -p "Press Enter to continue when you've configured .env..."
    else
        print_error "No environment file found. Please create .env file."
        exit 1
    fi
fi

# Validate required environment variables
print_status "Validating environment variables..."

required_vars=(
    "POSTGRES_URL"
    "LICITA_YA_API_KEY"
    "OPENAI_API_KEY"
)

source .env
missing_vars=()

for var in "${required_vars[@]}"; do
    if [[ -z "${!var}" || "${!var}" == *"YOUR_"* ]]; then
        missing_vars+=("$var")
    fi
done

if [[ ${#missing_vars[@]} -gt 0 ]]; then
    print_error "Missing or incomplete environment variables:"
    for var in "${missing_vars[@]}"; do
        echo "  - $var"
    done
    print_error "Please configure these variables in .env file"
    exit 1
fi

print_success "Environment validation passed"

# Build profiles for docker-compose
profiles=""
if [[ "$WITH_DASHBOARD" == true ]]; then
    profiles="$profiles --profile dashboard"
fi

if [[ "$WITH_REDIS" == true ]]; then
    profiles="$profiles --profile redis"
fi

# Docker compose file selection
compose_file="docker-compose.prod.yml"
if [[ "$ENVIRONMENT" == "staging" ]]; then
    compose_file="docker-compose.staging.yml"
    if [[ ! -f "$compose_file" ]]; then
        print_warning "Staging compose file not found, using production"
        compose_file="docker-compose.prod.yml"
    fi
fi

# Stop existing containers
print_status "Stopping existing containers..."
docker-compose -f "$compose_file" down || true

# Build images
print_status "Building Docker images..."
docker-compose -f "$compose_file" build

# Create necessary directories
print_status "Creating directories..."
mkdir -p logs
chmod 755 logs

# Start services
print_status "Starting services..."
if [[ "$DETACHED" == true ]]; then
    docker-compose -f "$compose_file" $profiles up -d
else
    docker-compose -f "$compose_file" $profiles up
fi

# Wait for services to be ready
if [[ "$DETACHED" == true ]]; then
    print_status "Waiting for services to be ready..."
    sleep 10

    # Check container health
    print_status "Checking container health..."
    if docker-compose -f "$compose_file" ps | grep -q "unhealthy"; then
        print_error "Some containers are unhealthy"
        docker-compose -f "$compose_file" ps
        exit 1
    fi

    # Test database connection
    print_status "Testing database connection..."
    if docker exec licitaciones_extractor_prod python src/main.py --mode=test; then
        print_success "Database connection test passed"
    else
        print_error "Database connection test failed"
        exit 1
    fi

    # Show running services
    print_success "Deployment completed successfully!"
    echo ""
    print_status "Running services:"
    docker-compose -f "$compose_file" ps

    echo ""
    print_status "Useful commands:"
    echo "  View logs: docker-compose -f $compose_file logs -f"
    echo "  Check status: docker-compose -f $compose_file ps"
    echo "  Stop services: docker-compose -f $compose_file down"
    echo "  Run tests: docker exec licitaciones_extractor_prod python src/main.py --mode=test"

    if [[ "$WITH_DASHBOARD" == true ]]; then
        echo "  Dashboard: http://localhost:5000"
    fi

    echo ""
    print_status "Log files location: ./logs/"
    print_success "Licitaciones Extractor is now running in production mode!"
fi