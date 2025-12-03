# Arreglar IDs de la Base de Datos

Si los IDs en la tabla `updates` no empiezan desde 1, puedes arreglarlos usando estos comandos.

## 🔍 **Verificar el estado actual:**

```bash
# Localmente
python src/main.py --mode=test

# En Railway
python src/main.py --mode=fix-ids
```

## 🔧 **Arreglar la secuencia de IDs:**

### **Opción 1: Desde main.py (Recomendado)**
```bash
# Arreglar IDs para que empiecen desde 1
python src/main.py --mode=fix-ids
```

### **Opción 2: Script independiente**
```bash
# Solo verificar
python scripts/fix_id_sequence.py --check

# Arreglar
python scripts/fix_id_sequence.py --fix
```

## 🎯 **En Railway después del deployment:**

1. **Ve al dashboard de Railway**
2. **Abre la consola de tu app**
3. **Ejecuta:**
   ```bash
   python src/main.py --mode=fix-ids
   ```

## 📊 **Qué hace el fix:**

### **Si la tabla está vacía:**
- ✅ Resetea la secuencia para empezar desde 1
- ✅ Los nuevos registros tendrán IDs 1, 2, 3, etc.

### **Si la tabla tiene datos:**
- ✅ Recrea los IDs empezando desde 1
- ✅ Mantiene todos los datos intactos
- ✅ Solo cambia los números de ID
- ✅ Preserva todas las relaciones

## ⚠️ **Importante:**

- **El proceso es seguro** - no se pierden datos
- **Se ejecuta en una transacción** - si falla, no se aplican cambios
- **Recomendado ejecutar en Railway** después del primer deployment
- **No es necesario si empiezas con base de datos limpia**

## 🔍 **Verificar que funcionó:**

```bash
# Verificar el estado después del fix
python src/main.py --mode=test

# Deberías ver:
# ✅ IDs start from 1 correctly
# ✅ No gaps in ID sequence
```

## 📈 **Ejemplo de antes y después:**

**Antes:**
```
IDs: 1001, 1002, 1003, 1004, ...
```

**Después:**
```
IDs: 1, 2, 3, 4, ...
```

## 🚨 **Si hay problemas:**

1. **Verificar conexión a la base de datos**
2. **Verificar que tienes permisos de ALTER TABLE**
3. **Ver los logs para detalles del error**

El fix está integrado en el sistema principal para facilidad de uso en producción.