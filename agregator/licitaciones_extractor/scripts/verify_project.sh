#!/bin/bash

# Project Verification Script
# Verifies that the project is clean and ready for production

set -e

GREEN='\033[0;32m'
RED='\033[0;31m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

print_success() {
    echo -e "${GREEN}✅ $1${NC}"
}

print_error() {
    echo -e "${RED}❌ $1${NC}"
}

print_warning() {
    echo -e "${YELLOW}⚠️  $1${NC}"
}

echo "🔍 Verificando limpieza del proyecto..."
echo ""

errors=0

# Check for test files
echo "📋 Verificando archivos de prueba..."
if ls *test*.py 2>/dev/null || ls test_*.py 2>/dev/null; then
    print_error "Se encontraron archivos de prueba"
    errors=$((errors + 1))
else
    print_success "No hay archivos de prueba"
fi

# Check for development documentation
echo ""
echo "📄 Verificando documentación de desarrollo..."
dev_docs=(
    "PHASE*.md"
    "*SUMMARY.md"
    "IMPLEMENTACION*.md"
    "*COMPLETION*.md"
    "PRODUCTION_IMPROVEMENTS.md"
)

found_dev_docs=false
for pattern in "${dev_docs[@]}"; do
    if ls $pattern 2>/dev/null; then
        found_dev_docs=true
        break
    fi
done

if $found_dev_docs; then
    print_error "Se encontraron archivos de documentación de desarrollo"
    errors=$((errors + 1))
else
    print_success "No hay documentación de desarrollo"
fi

# Check for cache directories
echo ""
echo "🗂️ Verificando directorios de cache..."
cache_dirs=(".pytest_cache" "__pycache__" ".coverage" "htmlcov")
found_cache=false
for dir in "${cache_dirs[@]}"; do
    if [[ -d "$dir" ]]; then
        found_cache=true
        print_error "Directorio de cache encontrado: $dir"
        errors=$((errors + 1))
    fi
done

if ! $found_cache; then
    print_success "No hay directorios de cache"
fi

# Check for log files in logs directory
echo ""
echo "📝 Verificando logs..."
if ls logs/*.log 2>/dev/null; then
    print_warning "Se encontraron archivos de log (normal en desarrollo)"
else
    print_success "Directorio de logs limpio"
fi

# Check for required production files
echo ""
echo "🏗️ Verificando archivos de producción..."
required_files=(
    "Dockerfile"
    "docker-compose.prod.yml"
    "DEPLOYMENT.md"
    ".env.production"
    "scripts/deploy.sh"
    "scripts/monitor.sh"
)

for file in "${required_files[@]}"; do
    if [[ -f "$file" ]]; then
        print_success "$file presente"
    else
        print_error "$file no encontrado"
        errors=$((errors + 1))
    fi
done

# Check for sensitive data in .env files
echo ""
echo "🔐 Verificando configuración..."
if [[ -f ".env" ]]; then
    if grep -q "YOUR_.*_HERE" .env; then
        print_warning ".env contiene placeholders - configurar antes del deployment"
    else
        print_success ".env configurado"
    fi
else
    print_warning ".env no existe - crear desde .env.production"
fi

# Check project structure
echo ""
echo "📁 Verificando estructura del proyecto..."
required_dirs=("src" "scripts" "logs")
for dir in "${required_dirs[@]}"; do
    if [[ -d "$dir" ]]; then
        print_success "Directorio $dir presente"
    else
        print_error "Directorio $dir no encontrado"
        errors=$((errors + 1))
    fi
done

# Check executable scripts
echo ""
echo "⚙️ Verificando scripts ejecutables..."
scripts=("scripts/deploy.sh" "scripts/monitor.sh")
for script in "${scripts[@]}"; do
    if [[ -x "$script" ]]; then
        print_success "$script es ejecutable"
    else
        print_error "$script no es ejecutable"
        errors=$((errors + 1))
    fi
done

echo ""
echo "=" * 50

if [[ $errors -eq 0 ]]; then
    print_success "✨ PROYECTO LIMPIO Y LISTO PARA PRODUCCIÓN ✨"
    echo ""
    echo "🚀 Para comenzar el deployment:"
    echo "   1. cp .env.production .env"
    echo "   2. Editar .env con sus credenciales"
    echo "   3. ./scripts/deploy.sh --dashboard"
    echo ""
    exit 0
else
    print_error "❌ PROYECTO REQUIERE LIMPIEZA ($errors errores encontrados)"
    echo ""
    echo "Por favor, corregir los errores antes del deployment"
    exit 1
fi