# Licitaciones Extractor - Deployment Guide

## 🚀 Quick Start

### Prerequisites
- Docker and Docker Compose
- PostgreSQL database (Neon recommended)
- API Keys (Licita Ya, OpenAI)

### Production Deployment

1. **Clone and Setup**
   ```bash
   git clone <repository-url>
   cd licitaciones_extractor
   ```

2. **Configure Environment**
   ```bash
   cp .env.production .env
   # Edit .env with your actual credentials
   ```

3. **Deploy with Docker**
   ```bash
   # Basic deployment (scheduler only)
   docker-compose -f docker-compose.prod.yml up -d

   # With dashboard
   docker-compose -f docker-compose.prod.yml --profile dashboard up -d

   # With Redis caching
   docker-compose -f docker-compose.prod.yml --profile redis up -d
   ```

4. **Verify Deployment**
   ```bash
   # Check container health
   docker-compose -f docker-compose.prod.yml ps

   # View logs
   docker-compose -f docker-compose.prod.yml logs -f licitaciones-extractor

   # Test system
   docker exec licitaciones_extractor_prod python src/main.py --mode=test
   ```

## 🏗️ Deployment Options

### Option 1: Docker Compose (Recommended)
- **Use case**: Single server deployment
- **Pros**: Easy setup, built-in orchestration
- **Cons**: Single point of failure

### Option 2: Kubernetes
- **Use case**: Production cluster
- **Pros**: High availability, auto-scaling
- **Cons**: More complex setup

### Option 3: Cloud Services
- **Use case**: Managed infrastructure
- **Pros**: Fully managed, scalable
- **Cons**: Vendor lock-in

## 🔧 Configuration

### Required Environment Variables
```bash
# Database
POSTGRES_URL=postgresql://user:pass@host:port/db

# API Keys
LICITA_YA_API_KEY=your_key
OPENAI_API_KEY=your_key

# Schedule
EXTRACTION_TIME=06:00
TIMEZONE=America/Mexico_City
```

### Optional Configuration
```bash
# Logging
LOG_LEVEL=INFO
LOG_STRUCTURED=true

# Performance
BATCH_SIZE=100
MAX_WORKERS=4

# Monitoring
SLACK_WEBHOOK_URL=your_webhook
ALERT_EMAIL=your_email
```

## 📊 Monitoring

### Health Checks
```bash
# Container health
docker exec licitaciones_extractor_prod python src/main.py --mode=test

# Database status
docker exec licitaciones_extractor_prod python src/main.py --mode=monitor

# Dashboard (if enabled)
curl http://localhost:5000/health
```

### Logs
```bash
# Application logs
docker-compose logs -f licitaciones-extractor

# Structured logs location
./logs/licitaciones_extractor_YYYYMMDD.log
```

## 🔄 Maintenance

### Updates
```bash
# Pull latest code
git pull origin main

# Rebuild and restart
docker-compose -f docker-compose.prod.yml build
docker-compose -f docker-compose.prod.yml up -d
```

### Database Backup
```bash
# Backup (adjust connection details)
pg_dump $POSTGRES_URL > backup_$(date +%Y%m%d).sql

# Restore
psql $POSTGRES_URL < backup_YYYYMMDD.sql
```

### Log Rotation
```bash
# Manual cleanup (automatic with loguru)
find ./logs -name "*.log" -mtime +30 -delete
```

## 🚨 Troubleshooting

### Common Issues

1. **Database Connection Failed**
   ```bash
   # Check database URL
   echo $POSTGRES_URL

   # Test connection
   docker exec licitaciones_extractor_prod python src/main.py --mode=test
   ```

2. **API Rate Limiting**
   ```bash
   # Check logs for rate limit errors
   docker logs licitaciones_extractor_prod | grep "rate"

   # Adjust retry settings in .env
   RETRY_ATTEMPTS=5
   RETRY_DELAY=2.0
   ```

3. **Chrome/Selenium Issues**
   ```bash
   # Check Chrome installation
   docker exec licitaciones_extractor_prod which chromium

   # Check chromedriver
   docker exec licitaciones_extractor_prod which chromedriver
   ```

4. **Memory Issues**
   ```bash
   # Check container memory
   docker stats licitaciones_extractor_prod

   # Reduce batch size
   BATCH_SIZE=50
   ```

### Debug Mode
```bash
# Run with debug logging
docker run --env LOG_LEVEL=DEBUG licitaciones-extractor

# Interactive debugging
docker exec -it licitaciones_extractor_prod /bin/bash
```

## 🛡️ Security

### Production Checklist
- [ ] Environment variables secured
- [ ] Database uses SSL
- [ ] Non-root container user
- [ ] Limited network access
- [ ] Regular security updates
- [ ] Log monitoring for anomalies

### Network Security
```yaml
# docker-compose.yml security settings
services:
  app:
    networks:
      - internal
    # Don't expose unnecessary ports
```

## 📈 Performance Tuning

### Database Optimization
```sql
-- Create indexes for better performance
CREATE INDEX CONCURRENTLY idx_updates_fecha_extraccion_fuente
ON updates(fecha_extraccion, fuente);

-- Analyze query performance
EXPLAIN ANALYZE SELECT * FROM updates
WHERE fecha_extraccion >= CURRENT_DATE - INTERVAL '7 days';
```

### Application Tuning
```bash
# Increase database pool size for high load
DB_POOL_SIZE=20
DB_MAX_OVERFLOW=40

# Optimize extraction settings
BATCH_SIZE=200
MAX_WORKERS=8
```

## 🔄 Backup & Recovery

### Automated Backups
```bash
#!/bin/bash
# backup.sh - Run this daily via cron

DATE=$(date +%Y%m%d_%H%M%S)
BACKUP_DIR="/backups"

# Database backup
pg_dump $POSTGRES_URL | gzip > "$BACKUP_DIR/db_backup_$DATE.sql.gz"

# Application backup
tar -czf "$BACKUP_DIR/app_backup_$DATE.tar.gz" /app/logs /app/.env

# Cleanup old backups (keep 30 days)
find $BACKUP_DIR -name "*.gz" -mtime +30 -delete
```

### Disaster Recovery
1. **Database Recovery**: Restore from latest backup
2. **Application Recovery**: Redeploy with latest code
3. **Data Recovery**: Re-run extraction for missing dates

## 🎯 Scaling

### Horizontal Scaling
```yaml
# docker-compose.yml - Multiple workers
services:
  worker-1:
    image: licitaciones-extractor
    environment:
      - WORKER_ID=1
  worker-2:
    image: licitaciones-extractor
    environment:
      - WORKER_ID=2
```

### Load Balancing
- Use nginx or HAProxy
- Database connection pooling
- Redis for caching

## 📞 Support

### Getting Help
1. Check logs first: `docker logs container_name`
2. Review configuration: `docker exec container env`
3. Test components: `python src/main.py --mode=test`
4. Create GitHub issue with logs and config

### Monitoring Alerts
Configure alerts for:
- Extraction failures
- Database connection issues
- High memory usage
- API rate limiting
- Data quality issues

---

## ✅ Production Deployment Checklist

- [ ] Environment variables configured
- [ ] Database connection tested
- [ ] API keys validated
- [ ] Docker containers healthy
- [ ] Logs are flowing
- [ ] Monitoring dashboard accessible
- [ ] Scheduled extraction running
- [ ] Backup strategy implemented
- [ ] Security measures applied
- [ ] Performance tuned
- [ ] Team trained on operations

**🎉 Your Licitaciones Extractor is now ready for production!**